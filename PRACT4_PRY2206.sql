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
    v_run_cli       NUMBER(10);
    v_dv_cli        VARCHAR2(1);
    v_nombre_cli    VARCHAR2(100);
    v_tipo_cli      VARCHAR2(100);
    
    --VARIABLES ESCALARES DE TRABAJO
    v_anio_anterior NUMBER(4);
    v_monto_inicial NUMBER(12);
    v_puntos_base   NUMBER(12);
    v_puntos_extra  NUMBER(12);
    v_puntos_total  NUMBER(12);
    v_rate_extra    NUMBER(5);
    
    --CURSOR EXPLICITO CON PARAMETROS
    --RECIBE EL RUN Y EL AÑO PARA FILTRAR SOLO LO NECESARIO
    CURSOR c_detalles (p_run NUMBER, p_anio NUMBER) IS
        SELECT  
            tr.nro_tarjeta,
            tr.nro_transaccion,
            tr.fecha_transaccion,
            REPLACE(tt.nombre_tptran_tarjeta, 'S per', 'Súper') AS tipo_transaccion,
            tr.monto_total_transaccion
        FROM transaccion_tarjeta_cliente tr
        JOIN tarjeta_cliente tc ON tr.nro_tarjeta = tc.nro_tarjeta
        JOIN tipo_transaccion_tarjeta tt ON tr.cod_tptran_tarjeta = tt.cod_tptran_tarjeta
        WHERE tc.numrun = p_run
          AND EXTRACT(YEAR FROM tr.fecha_transaccion) = p_anio
        ORDER BY tr.fecha_transaccion;
    
    v_reg_det c_detalles%ROWTYPE;
    
BEGIN
    -- CONFIGURACIONES FECHA
    -- SE OBTIENE EL AÑO ANTERIOR A PARTIR DE LA FECHA PARAMETRICA
    v_anio_anterior := EXTRACT(YEAR FROM TO_DATE(:b_fecha_proceso, 'DD/MM/YYYY')) - 1;
    
    -- LIMPIEZA DE TABLAS DE RESULTADOS
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_PUNTOS_TARJETA_CATB';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_PUNTOS_TARJETA_CATB';
    
    -- APERTURA DEL CURSOR VARIABLE PARA CLIENTES CON TRANSACCIONES EL AÑO ANTERIOR
    OPEN c_clientes FOR
        SELECT DISTINCT 
               c.numrun,
               c.dvrun,
               tp.nombre_tipo_cliente
        FROM cliente c
        JOIN tarjeta_cliente tc ON c.numrun = tc.numrun
        JOIN transaccion_tarjeta_cliente tr ON tc.nro_tarjeta = tr.nro_tarjeta
        JOIN tipo_cliente tp ON c.cod_tipo_cliente = tp.cod_tipo_cliente
        WHERE EXTRACT(YEAR FROM tr.fecha_transaccion) = v_anio_anterior;
        
    LOOP
        FETCH c_clientes INTO v_run_cli, v_dv_cli, v_tipo_cli;
        EXIT WHEN c_clientes%NOTFOUND;
        
        -- CALCULO DEL MONTO ANUAL TOTAL DEL CLIENTE
        SELECT NVL(SUM(tr.monto_total_transaccion), 0)
        INTO v_monto_inicial
        FROM transaccion_tarjeta_cliente tr
        JOIN tarjeta_cliente tc ON tr.nro_tarjeta = tc.nro_tarjeta
        WHERE tc.numrun = v_run_cli
          AND EXTRACT(YEAR FROM tr.fecha_transaccion) = v_anio_anterior;
          
        -- DETERMINACION DE PUNTOS EXTRA SEGUN PERFIL DEL CLIENTE
        v_rate_extra := 0;
        
        IF v_tipo_cli LIKE '%Dueña%' OR v_tipo_cli LIKE '%Pensionado%' THEN
            IF v_monto_inicial > 900000 THEN
                v_rate_extra := v_valores_puntos(4);
            ELSIF v_monto_inicial > 700000 THEN
                v_rate_extra := v_valores_puntos(3);
            ELSIF v_monto_inicial > 500000 THEN
                v_rate_extra := v_valores_puntos(2);
            END IF;
        END IF;
        
        -- PROCESAMIENTO DE TRANSACCIONES DEL CLIENTE
        OPEN c_detalles(v_run_cli, v_anio_anterior);
        LOOP
            FETCH c_detalles INTO v_reg_det;
            EXIT WHEN c_detalles%NOTFOUND;
            
            -- CALCULO DE PUNTOS BASE Y EXTRA
            v_puntos_base  := TRUNC(v_reg_det.monto_total_transaccion / 100000) * v_valores_puntos(1);
            v_puntos_extra := TRUNC(v_reg_det.monto_total_transaccion / 100000) * v_rate_extra;
            v_puntos_total := v_puntos_base + v_puntos_extra;
            
            -- INSERTAMOS EN LA TABLA DETALLE DE TRANSACCIONES
            INSERT INTO DETALLE_PUNTOS_TARJETA_CATB
            (
                NUMRUN,
                DVRUN,
                NRO_TARJETA,
                NRO_TRANSACCION,
                FECHA_TRANSACCION,
                TIPO_TRANSACCION,
                MONTO_TRANSACCION,
                PUNTOS_ALLTHEBEST
            )
            VALUES
            (
                v_run_cli,
                v_dv_cli,
                v_reg_det.nro_tarjeta,
                v_reg_det.nro_transaccion,
                v_reg_det.fecha_transaccion,
                v_reg_det.tipo_transaccion,
                v_reg_det.monto_total_transaccion,
                v_puntos_total
            );
             
        END LOOP;
        CLOSE c_detalles;
    
    END LOOP;
    CLOSE c_clientes;
    
    -- INSERCION DE RESUMEN MENSUAL DE TRANSACCIONES Y PUNTOS
    INSERT INTO RESUMEN_PUNTOS_TARJETA_CATB
    (
        MES_ANNO,
        MONTO_TOTAL_COMPRAS,
        TOTAL_PUNTOS_COMPRAS,
        MONTO_TOTAL_AVANCES,
        TOTAL_PUNTOS_AVANCES,
        MONTO_TOTAL_SAVANCES,
        TOTAL_PUNTOS_SAVANCES
    )
    SELECT
        mes_anno,
        monto_total_compras,
        total_puntos_compras,
        monto_total_avances,
        total_puntos_avances,
        monto_total_savances,
        total_puntos_savances
    FROM (
        SELECT
            TO_CHAR(fecha_transaccion, 'MMYYYY') AS mes_anno,

            SUM(CASE WHEN tipo_transaccion LIKE '%Compra%' THEN monto_transaccion ELSE 0 END) AS monto_total_compras,
            SUM(CASE WHEN tipo_transaccion LIKE '%Compra%' THEN puntos_allthebest ELSE 0 END) AS total_puntos_compras,

            SUM(CASE WHEN tipo_transaccion LIKE '%Avance%' THEN monto_transaccion ELSE 0 END) AS monto_total_avances,
            SUM(CASE WHEN tipo_transaccion LIKE '%Avance%' THEN puntos_allthebest ELSE 0 END) AS total_puntos_avances,

            SUM(CASE WHEN tipo_transaccion LIKE '%Súper%' THEN monto_transaccion ELSE 0 END) AS monto_total_savances,
            SUM(CASE WHEN tipo_transaccion LIKE '%Súper%' THEN puntos_allthebest ELSE 0 END) AS total_puntos_savances

        FROM DETALLE_PUNTOS_TARJETA_CATB
        GROUP BY TO_CHAR(fecha_transaccion, 'MMYYYY')
        ORDER BY TO_CHAR(fecha_transaccion, 'YYYYMM')
    );
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Caso 1: Proceso de puntos completado');
    
END;
/
--SELECT * FROM DETALLE_PUNTOS_TARJETA_CATB;
--SELECT * FROM RESUMEN_PUNTOS_TARJETA_CATB ORDER BY mes_anno;


/*===============================================================
CASO 2: CALCULO APORTE SBIF
OBJ: CALCULAR AUTOMATICAMENTE LOS MONTOS DE APORTE A LA SBIF
BASADO EN LAS TRANSACCIONES DE AVANCES Y SUPER AVANCES REALIZADOS DURANTE EL AÑO ANTERIOR
GENERANDO Y ALMACENANDO LA INFORMACION DETALLADA Y RESUMIDA EN LAS TABLAS PARA CUMPLIR CON LA NORMA VIGENTE
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
    
    -- CURSOR 1 (EXPLICITO): RESUMEN
    -- OBJETIVO: AGRUPAR POR MES Y TIPO DE TRANSACCION (AVANCE O SUPER AVANCE)
    -- ESTO PERMITE LLENAR LA TABLA RESUMEN_APORTE_SBIF
    CURSOR C_RESUMEN IS
        SELECT 
            TO_CHAR(TR.FECHA_TRANSACCION, 'MMYYYY') AS MES_ANNO,
            TT.COD_TPTRAN_TARJETA,
            REPLACE(TT.NOMBRE_TPTRAN_TARJETA, 'S per', 'Súper') AS NOMBRE_TIPO
        FROM TRANSACCION_TARJETA_CLIENTE TR
        JOIN TIPO_TRANSACCION_TARJETA TT 
             ON TR.COD_TPTRAN_TARJETA = TT.COD_TPTRAN_TARJETA
        WHERE EXTRACT(YEAR FROM TR.FECHA_TRANSACCION) = EXTRACT(YEAR FROM SYSDATE)
          AND (TT.NOMBRE_TPTRAN_TARJETA LIKE '%Avance%' 
               OR TT.NOMBRE_TPTRAN_TARJETA LIKE '%Súper%')
        GROUP BY TO_CHAR(TR.FECHA_TRANSACCION, 'MMYYYY'),
                 TT.COD_TPTRAN_TARJETA,
                 TT.NOMBRE_TPTRAN_TARJETA
        ORDER BY MES_ANNO ASC, NOMBRE_TIPO ASC;
        
    -- REGISTRO PARA CURSOR RESUMEN
    V_REG_RES C_RESUMEN%ROWTYPE;
    
    -- CURSOR 2 (EXPLICITO CON PARAMETROS): DETALLE
    -- OBJETIVO: OBTENER EL DETALLE DE TRANSACCIONES POR MES Y TIPO
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
        JOIN CLIENTE C ON TC.NUMRUN = C.NUMRUN
        WHERE TO_CHAR(TR.FECHA_TRANSACCION, 'MMYYYY') = P_MES
          AND TR.COD_TPTRAN_TARJETA = P_COD_TIPO
        ORDER BY TR.FECHA_TRANSACCION ASC, TC.NUMRUN ASC;
        
    -- REGISTRO PARA CURSOR DETALLE
    V_REG_DET C_DETALLE%ROWTYPE;

BEGIN
    -- OBTENEMOS EL AÑO DEL PROCESO
    V_ANIO_PROCESO := EXTRACT(YEAR FROM SYSDATE);
    DBMS_OUTPUT.PUT_LINE('INICIANDO PROCESO PARA EL AÑO: ' || V_ANIO_PROCESO);
    
    -- LIMPIEZA DE TABLAS DE DESTINO
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
    
    -- RECORRIDO DEL CURSOR PRINCIPAL
    OPEN C_RESUMEN;
    LOOP
        FETCH C_RESUMEN INTO V_REG_RES;
        EXIT WHEN C_RESUMEN%NOTFOUND;
        
        -- REINICIO DE ACUMULADORES POR MES Y TIPO
        V_ACUM_MONTO  := 0;
        V_ACUM_APORTE := 0;
        
        -- RECORRIDO DEL CURSOR DETALLE
        OPEN C_DETALLE(V_REG_RES.MES_ANNO, V_REG_RES.COD_TPTRAN_TARJETA);
        LOOP
            FETCH C_DETALLE INTO V_REG_DET;
            EXIT WHEN C_DETALLE%NOTFOUND;
            
            -- OBTENCION DEL PORCENTAJE DE APORTE SBIF
            -- SE UTILIZA NVL Y MAX PARA EVITAR USO DE EXCEPTION (CONTENIDO SEMANA POSTERIOR)
            SELECT NVL(MAX(PORC_APORTE_SBIF), 0)
            INTO V_PORCENTAJE
            FROM TRAMO_APORTE_SBIF
            WHERE V_REG_DET.MONTO_TOTAL_TRANSACCION >= TRAMO_INF_AV_SAV
              AND (TRAMO_SUP_AV_SAV IS NULL 
                   OR V_REG_DET.MONTO_TOTAL_TRANSACCION <= TRAMO_SUP_AV_SAV);
            
            -- CALCULO DEL MONTO DE APORTE
            V_MONTO_APORTE := ROUND(V_REG_DET.MONTO_TOTAL_TRANSACCION * V_PORCENTAJE / 100);
            
            -- ACUMULACION PARA RESUMEN
            V_ACUM_MONTO  := V_ACUM_MONTO  + V_REG_DET.MONTO_TOTAL_TRANSACCION;
            V_ACUM_APORTE := V_ACUM_APORTE + V_MONTO_APORTE;
            
            -- INSERCION EN TABLA DETALLE
            INSERT INTO DETALLE_APORTE_SBIF
            (NUMRUN, DVRUN, NRO_TARJETA, NRO_TRANSACCION, FECHA_TRANSACCION,
             TIPO_TRANSACCION, MONTO_TOTAL_TRANSACCION, APORTE_SBIF)
            VALUES
            (V_REG_DET.NUMRUN, V_REG_DET.DVRUN, V_REG_DET.NRO_TARJETA,
             V_REG_DET.NRO_TRANSACCION, V_REG_DET.FECHA_TRANSACCION,
             V_REG_RES.NOMBRE_TIPO, V_REG_DET.MONTO_TOTAL_TRANSACCION,
             V_MONTO_APORTE);
             
            V_TOTAL_FILAS := V_TOTAL_FILAS + 1;
            
        END LOOP;
        CLOSE C_DETALLE;
        
        -- INSERCION EN TABLA RESUMEN
        INSERT INTO RESUMEN_APORTE_SBIF
        (MES_ANNO, TIPO_TRANSACCION, MONTO_TOTAL_TRANSACCIONES, APORTE_TOTAL_ABIF)
        VALUES
        (V_REG_RES.MES_ANNO, V_REG_RES.NOMBRE_TIPO,
         V_ACUM_MONTO, V_ACUM_APORTE);
        
    END LOOP;
    CLOSE C_RESUMEN;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('PROCESO FINALIZADO EXITOSAMENTE. FILAS PROCESADAS: ' || V_TOTAL_FILAS);

END;
/
--SELECT * FROM DETALLE_APORTE_SBIF ORDER BY FECHA_TRANSACCION;
--SELECT * FROM RESUMEN_APORTE_SBIF ORDER BY MES_ANNO, TIPO_TRANSACCION;

