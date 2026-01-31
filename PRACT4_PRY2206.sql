/*SEMANA 4: ALL THE BEST
CASO 1: DETALLE DE PUNTOS "CLUB DE AMIGOS THE BEST"

OBJ: CALCULAR EL PUNTAJE ACUMULADO POR LOS LCIENTES BASADO EN SUS COMPRAS
DEL AÑO ANTEIOR. LOGICA DE PUNTOS BASE Y PUNTOS EXTRA SEGUN EL PERFIL DE CADA CLIENTE
Y SU MONTO ANUAL DE COMPRAS*/

--1.- VARIABLE BIND 
VARIABLE b_fecha_proceso VARCHAR2(10);
EXEC :b_fecha_proceso := '15/07/2026';

DECLARE
    --DEFINICION DE TIPOS Y ESTRUCTURA
    --VARRAY PARA LOS VALORES DE PUNTOS
    
    TYPE t_arr_puntos IS VARRAY(4) OF NUMBER;
    v_valores_puntos t_arr_puntos := t_arr_puntos(250,300,550,700);
    
    --DEFINIMOS EL REF CURSOR PARA LOS CLIENTES
    TYPE t_cursor_cli IS REF CURSOR;
    c_clientes t_cursor_cli;
    
    --VARIABLES PARA RECIBIR DATOS DEL CURSOR
    v_run_cli   NUMBER(10);
    v_dv_cli    VARCHAR(1);
    v_nombre_cli    VARCHAR2(100);
    v_tipo_cli  VARCHAR2(100);
    
    --VARIABLES ESCALARES DE TRABAJO
    v_anio_anterior     NUMBER(4);
    v_monto_inicial      NUMBER(12);
    v_puntos_base       NUMBER(12);
    v_puntos_extra      NUMBER(12);
    v_puntos_total      NUMBER(12);
    v_rate_extra        NUMBER(5);
    
    --CURSOR EXPLICITO CON PARAMETROS
    --RECIBE EL RUN Y EL AÑO PARA FILTRAR SOLO LO NECESARIO
    CURSOR c_detalles (p_run  NUMBER, p_anio  NUMBER) IS
        SELECT  
            tr.nro_tarjeta,
            tr.nro_transaccion,
            tr.FECHA_TRANSACCION,
            REPLACE(tt.NOMBRE_TPTRAN_TARJETA, 'S per', 'Súper') AS tipo_transaccion,
            tr.MONTO_TOTAL_TRANSACCION
        FROM TRANSACCION_TARJETA_CLIENTE tr
        JOIN tarjeta_cliente tc ON tr.nro_tarjeta = tc.nro_tarjeta
        JOIN tipo_transaccion_tarjeta tt ON tr.cod_tptran_tarjeta = tt.cod_tptran_tarjeta
        WHERE tc.numrun = p_run
          AND EXTRACT(YEAR FROM tr.fecha_transaccion) = p_anio
        ORDER BY tr.fecha_transaccion;
    
    v_reg_det  c_detalles%ROWTYPE;
    
BEGIN
    -- CONFIGURACIONES FECHA
    v_anio_anterior := EXTRACT (YEAR FROM TO_DATE(:b_fecha_proceso, 'DD/MM/YYYY')) - 1;
    
    --LIMPIAMOS LAS TABLAS
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_PUNTOS_TARJETA_CATB';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_PUNTOS_TARJETA_CATB';
    
    
    -- REALIZAMOS LA APERTURA DEL CURSOR VARIABLE PARA RECORRER LOS CLIENTES CON MOVIMIENTO EN SUS CUENTAS
    OPEN c_clientes FOR
        SELECT DISTINCT c.numrun, c.dvrun, tp.nombre_tipo_cliente
        FROM cliente c
        JOIN tarjeta_cliente tc ON c.numrun = tc.numrun
        JOIN transaccion_tarjeta_cliente tr ON tc.nro_tarjeta = tr.nro_tarjeta
        JOIN tipo_cliente tp ON c.cod_tipo_cliente = tp.cod_tipo_cliente
        WHERE EXTRACT(YEAR FROM tr.fecha_transaccion) = v_anio_anterior;
        
    LOOP
        FETCH c_clientes INTO v_run_cli, v_dv_cli, v_tipo_cli;
        EXIT WHEN c_clientes%NOTFOUND;
        
        --CALCULAMOS EL MONTO ANUUAL PARA DEFINIR SI GANA PUNTOS EXTRA
        SELECT NVL(SUM(tr.monto_total_transaccion), 0)
        INTO v_monto_inicial
        FROM transaccion_tarjeta_cliente tr
        JOIN tarjeta_cliente tc ON tr.nro_tarjeta = tc.nro_tarjeta
        WHERE tc.numrun = v_run_cli
          AND EXTRACT(YEAR FROM tr.fecha_transaccion) = v_anio_anterior;
          
        --DEFINIMOS LA TASA DE PUNTOS EXTRA CON LAS REGLAS CORRESPONDIENTES
        -- SOLO DUEÑAS DE CASA Y TERCERA EDAD RECIBEN PUNTOS EXTRA SI CUMPLEN CON LO REQUERIDO
        
        v_rate_extra := 0;
        IF v_tipo_cli LIKE '%Dueña%' OR v_tipo_cli LIKE '%Pensionado%' THEN
            IF v_monto_inicial > 900000 THEN
                v_rate_extra := v_valores_puntos(4);
            ELSIF v_monto_inicial > 700000 THEN
                v_rate_extra := v_valores_puntos(3);
            ELSIF v_monto_inicial > 500000 THEN
                v_rate_extra  := v_valores_puntos(2);
            END IF;
        END IF;
        
        -- PROCESAMOS LAS TRANSACCIONES
        OPEN c_detalles(v_run_cli, v_anio_anterior);
        LOOP
            FETCH c_detalles INTO v_reg_det;
            EXIT WHEN c_detalles%NOTFOUND;
            
            --CALCULAMOS LOS PUNTOS, USAREMOS TRUNC PARA CONSIDERAR TRAMOS COMPLETOS DE 100.000
            v_puntos_base := TRUNC(v_reg_det.monto_total_transaccion / 100000) * v_valores_puntos(1);
            v_puntos_extra := TRUNC (v_reg_det.monto_total_transaccion / 100000) * v_rate_extra;
            v_puntos_total := v_puntos_base + v_puntos_extra;
            
            --INSERTAMOS EN LA TABLA DETALLE
            INSERT INTO DETALLE_PUNTOS_TARJETA_CATB
            (NUMRUN,DVRUN,NRO_TARJETA,NRO_TRANSACCION,FECHA_TRANSACCION,TIPO_TRANSACCION,MONTO_TRANSACCION,PUNTOS_ALLTHEBEST)
            VALUES
            (v_run_cli, v_dv_cli, v_reg_det.nro_tarjeta, v_reg_det.nro_transaccion,
             v_reg_det.fecha_transaccion, v_reg_det.tipo_transaccion, 
             v_reg_det.monto_total_transaccion, v_puntos_total);
             
        END LOOP;
        CLOSE c_detalles;
    
    END LOOP;
    CLOSE c_clientes;
    
    --INSERTAMOS EN LA TABLA RESUMEN AGRUPANDO POR MES
    INSERT INTO RESUMEN_PUNTOS_TARJETA_CATB
    (MES_ANNO,MONTO_TOTAL_COMPRAS,TOTAL_PUNTOS_COMPRAS,MONTO_TOTAL_AVANCES,TOTAL_PUNTOS_AVANCES,MONTO_TOTAL_SAVANCES,TOTAL_PUNTOS_SAVANCES)
    SELECT
        TO_CHAR(fecha_transaccion, 'MMYYYY'),
        --COMPRAS
        SUM(CASE WHEN tipo_transaccion LIKE '%Compra%' THEN monto_transaccion ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion LIKE '%Compra%' THEN puntos_allthebest ELSE 0 END),
        --AVANCES
        SUM(CASE WHEN tipo_transaccion LIKE '%Avance%' THEN monto_transaccion ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion LIKE '%Avance%' THEN puntos_allthebest ELSE 0 END),
        --SUPER AVANCE
        SUM(CASE WHEN tipo_transaccion LIKE '%Súper%' THEN monto_transaccion ELSE 0 END),
        SUM(CASE WHEN tipo_transaccion LIKE '%Súper%' THEN puntos_allthebest ELSE 0 END)
    FROM DETALLE_PUNTOS_TARJETA_CATB
    GROUP BY TO_CHAR(fecha_transaccion, 'MMYYYY');
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Caso 1: Proceso de puntos completado');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR EN PROCESO: ' || SQLERRM);
END;
/
--SELECT * FROM DETALLE_PUNTOS_TARJETA_CATB ;
--SELECT * FROM RESUMEN_PUNTOS_TARJETA_CATB ORDER BY mes_anno;





/*===============================================================
CASO 2: CALCULO APORTE SBIF
OBJ: CALCULAR AUTOMATICAMENTE LOS MONTOS DE APORTE A LA SBIF
BASADO EN LAS TRANSACCIONES DE AVANCES Y SUPER AVANCES REALIZADOS DURANTE EL AÑO ANTERIO
GENERANDO Y ALMACENANDO LA INFORMACION DETALLADA Y RESUMIDA EN LAS TABLAS PAARA CUMPLIR CON LA NORMA VIEGENTE
=================================================================*/

DECLARE
    -- === 1. DECLARACION DE VARIABLES ===
    
    -- VARIABLE PARA ALMACENAR EL ANIO DE PROCESO 
    V_ANIO_PROCESO      NUMBER(4);
    
    -- VARIABLES PARA CALCULOS UNITARIOS
    V_PORCENTAJE        NUMBER(3);
    V_MONTO_APORTE      NUMBER(12);
    
    -- ACUMULADORES PARA LA TABLA DE RESUMEN
    V_ACUM_MONTO        NUMBER(12);
    V_ACUM_APORTE       NUMBER(12);
    
    -- CONTADOR DE FILAS PROCESADAS (PARA CONTROL)
    V_TOTAL_FILAS       NUMBER := 0;
    
    -- === 2. DECLARACION DE CURSORES ===
    
    -- CURSOR 1 (EXPLICTO): RESUMEN
    -- OBJETIVO: AGRUPAR POR MES Y TIPO DE TRANSACCION (AVANCE O SUPER AVANCE)
    -- ESTO PERMITE LLENAR LA TABLA RESUMEN_APORTE_SBIF AL FINAL DEL BUCLE
    CURSOR C_RESUMEN IS
        SELECT 
            TO_CHAR(TR.FECHA_TRANSACCION, 'MMYYYY') AS MES_ANNO,
            TT.COD_TPTRAN_TARJETA,
            REPLACE(TT.NOMBRE_TPTRAN_TARJETA, 'S per', 'Súper') AS NOMBRE_TIPO
        FROM TRANSACCION_TARJETA_CLIENTE TR
        JOIN TIPO_TRANSACCION_TARJETA TT ON TR.COD_TPTRAN_TARJETA = TT.COD_TPTRAN_TARJETA
        WHERE EXTRACT(YEAR FROM TR.FECHA_TRANSACCION) = EXTRACT(YEAR FROM SYSDATE) -- FILTRA SOLO EL ANIO ACTUAL
          AND (TT.NOMBRE_TPTRAN_TARJETA LIKE '%Avance%' OR TT.NOMBRE_TPTRAN_TARJETA LIKE '%Súper%')
        GROUP BY TO_CHAR(TR.FECHA_TRANSACCION, 'MMYYYY'), 
                 TT.COD_TPTRAN_TARJETA, 
                 TT.NOMBRE_TPTRAN_TARJETA
        ORDER BY MES_ANNO ASC, NOMBRE_TIPO ASC;
        
    -- VARIABLE TIPO ROWTYPE PARA ALMACENAR EL REGISTRO DEL CURSOR RESUMEN
    V_REG_RES C_RESUMEN%ROWTYPE;
    
    -- CURSOR 2 : DETALLE (con parametros9
    -- OBJETIVO: OBTENER EL DETALLE DE LAS TRANSACCIONES PARA UN MES Y TIPO ESPECIFICO
    
    CURSOR C_DETALLE (P_MES VARCHAR2, P_COD_TIPO NUMBER) IS
        SELECT 
            C.NUMRUN, 
            C.DVRUN, 
            TR.NRO_TARJETA, 
            TR.NRO_TRANSACCION, 
            TR.FECHA_TRANSACCION, 
            TR.MONTO_TOTAL_TRANSACCION
        FROM TRANSACCION_TARJETA_CLIENTE TR
        JOIN TARJETA_CLIENTE TC ON TR.NRO_TARJETA = TC.NRO_TARJETA
        JOIN CLIENTE c ON tc.numrun = c.numrun
        WHERE TO_CHAR(TR.FECHA_TRANSACCION, 'MMYYYY') = P_MES
          AND TR.COD_TPTRAN_TARJETA = P_COD_TIPO
        ORDER BY TR.FECHA_TRANSACCION ASC, TC.NUMRUN ASC;
        
    -- VARIABLE TIPO ROWTYPE PARA ALMACENAR EL REGISTRO DEL CURSOR DETALLE
    V_REG_DET C_DETALLE%ROWTYPE;

BEGIN
    -- INICIAMOS EL PROCESO DEL PROCESO
    
    -- OBTENEMOS EL AÑO ACTUAL DEL SISTEMA
    V_ANIO_PROCESO := EXTRACT(YEAR FROM SYSDATE);
    DBMS_OUTPUT.PUT_LINE('INICIANDO PROCESO PARA EL AÑO: ' || V_ANIO_PROCESO);
    
    -- LIMPIEZA DE TABLAS DE DESTINO 
    -- SE USA EXECUTE IMMEDIATE PORQUE TRUNCATE ES DDL
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
    
    -- APERTURA Y RECORRIDO DEL CURSOR PRINCIPAL 
    OPEN C_RESUMEN;
    LOOP
        FETCH C_RESUMEN INTO V_REG_RES;
        EXIT WHEN C_RESUMEN%NOTFOUND;
        
        -- REINICIAR ACUMULADORES POR CADA GRUPO (MES/TIPO)
        V_ACUM_MONTO := 0;
        V_ACUM_APORTE := 0;
        
        -- APERTURA Y RECORRIDO DEL CURSOR SECUNDARIO (DETALLE)
        -- SE PASAN LOS PARAMETROS OBTENIDOS DEL CURSOR C_RESUMEN
        OPEN C_DETALLE(V_REG_RES.MES_ANNO, V_REG_RES.COD_TPTRAN_TARJETA);
        LOOP
            FETCH C_DETALLE INTO V_REG_DET;
            EXIT WHEN C_DETALLE%NOTFOUND;
            
            -- LOGICA DE NEGOCIO: CALCULO DEL PORCENTAJE DE APORTE
            -- SE BUSCA EL PORCENTAJE EN LA TABLA TRAMO_APORTE_SBIF SEGUN EL MONTO
            V_PORCENTAJE := 0; -- VALOR POR DEFECTO
            
            BEGIN
                SELECT PORC_APORTE_SBIF 
                INTO V_PORCENTAJE
                FROM TRAMO_APORTE_SBIF
                WHERE V_REG_DET.MONTO_TOTAL_TRANSACCION BETWEEN TRAMO_INF_AV_SAV AND TRAMO_SUP_AV_SAV;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    V_PORCENTAJE := 0; -- SI NO EXISTE TRAMO, EL APORTE ES 0
            END;
            
            -- CALCULO DEL MONTO APORTE (MONTO * PORCENTAJE / 100)
            V_MONTO_APORTE := ROUND(V_REG_DET.MONTO_TOTAL_TRANSACCION * V_PORCENTAJE / 100);
            
            -- ACTUALIZAR ACUMULADORES GLOBALES DEL GRUPO
            V_ACUM_MONTO := V_ACUM_MONTO + V_REG_DET.MONTO_TOTAL_TRANSACCION;
            V_ACUM_APORTE := V_ACUM_APORTE + V_MONTO_APORTE;
            
            -- INSERTAMOS LOS DATOS EN LA TABLA DE DETALLE
            INSERT INTO DETALLE_APORTE_SBIF
            (NUMRUN, DVRUN, NRO_TARJETA, NRO_TRANSACCION, FECHA_TRANSACCION, 
             TIPO_TRANSACCION, MONTO_TRANSACCION, APORTE_SBIF)
            VALUES
            (V_REG_DET.NUMRUN, V_REG_DET.DVRUN, V_REG_DET.NRO_TARJETA, V_REG_DET.NRO_TRANSACCION,
             V_REG_DET.FECHA_TRANSACCION, V_REG_RES.NOMBRE_TIPO, 
             V_REG_DET.MONTO_TOTAL_TRANSACCION, V_MONTO_APORTE);
             
             V_TOTAL_FILAS := V_TOTAL_FILAS + 1;
             
        END LOOP;
        -- CERRAMOS EL  CURSOR DE DETALLE PARA LIBERAR MEMORIA
        CLOSE C_DETALLE;
        
        -- INSERTAMOS LOS DATOS EN LA TABLA DE RESUMEN
        INSERT INTO RESUMEN_APORTE_SBIF
        (MES_ANNO, TIPO_TRANSACCION, MONTO_TOTAL_TRANSACCIONES, APORTE_TOTAL_ABIF)
        VALUES
        (V_REG_RES.MES_ANNO, V_REG_RES.NOMBRE_TIPO, V_ACUM_MONTO, V_ACUM_APORTE);
        
    END LOOP;
    -- CERRAMOS CURSOR PRINCIPAL
    CLOSE C_RESUMEN;
    
    -- CONFIRMAR TRANSACCION
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('PROCESO FINALIZADO EXITOSAMENTE. FILAS PROCESADAS: ' || V_TOTAL_FILAS);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK; -- DESHACER CAMBIOS EN CASO DE ERROR
        DBMS_OUTPUT.PUT_LINE('ERROR CRITICO EN EL PROCESO: ' || SQLERRM);
END;
/

-- CONSULTAS PARA VERIFICAR LOS RESULTADOS
--SELECT * FROM DETALLE_APORTE_SBIF ORDER BY FECHA_TRANSACCION;
--SELECT * FROM RESUMEN_APORTE_SBIF ORDER BY MES_ANNO;



