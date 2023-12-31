import pandas as pd
import os

import sqlalchemy as sa


FILENAME = 'admision-univ-121223.csv'
INPUT_PATH = 'server_inputs/'
OUTPUT_PATH = 'server_outputs/'

DB_NAME = 'pea-datascience'
DB_LOCATION = f'mysql+mysqlconnector://root:root@localhost:3306/{DB_NAME}'

COL_DEUDAS = {
        'CodigoSBS': 'Cod_SBS',
        'CodigoEmpresa': 'Cod_Emp',
        'TipoCredito': 'Tip_Credit',
        'ValorSaldo': 'Val_Saldo',
        'ClasificacionDeuda': 'Clasif_deu',
        'CodigoCuenta': 'Cod_Cuenta'
    }


def extract():
    df_admision = pd.read_csv(INPUT_PATH + FILENAME, encoding="ISO-8859-1")
    return df_admision


def transform(df):
    df_resultado = pd.DataFrame()
    # Llenamos los campos de clientes
    df_resultado['NombreAlumno'] = df['NOMBRES'].astype('str')
    df_resultado['CodSed'] = df['COD_SED'].astype('str')
    df_resultado['SemApe'] = df['SEM_APE'].astype('str')
    df_resultado['CodEsp'] = df['COD_ESP'].astype('str')
    df_resultado['FechaExamen'] = df['FECH_EXAM'].astype('str')
    df_resultado['HoraExamen'] = df['HORA_EXAM'].astype('str')
    df_resultado['Sexo'] = df['SEXO'].astype('str')
    df_resultado['FechaNacimiento'] = df['FNACIMIEN'].astype('str')
    df_resultado['DiaNacimiento'] = df['DIANACIMI'].astype('str')
    df_resultado['MesNacimiento'] = df['MESNACIMI'].astype('str')
    df_resultado['AnoNacimiento'] = df['ANNONACIM'].astype('str')
    df_resultado['CodigoModalidad'] = df['CODMOD'].astype('str')
    df_resultado['Modalidad'] = df['MODALIDAD'].astype('str')
    df_resultado['Sede'] = df['SEDE'].astype('str')
    df_resultado['Semestre'] = df['SEMESTRE'].astype('str')
    df_resultado['Especialidad'] = df['ESPECIALIDAD'].astype('str')
    df_resultado['CodFac'] = df['CODFAC'].astype('str')
    df_resultado['Facultad'] = df['FACULTAD'].astype('str')
    df_resultado['Puntaje'] = df['PUNTAJE'].astype('str')
    df_resultado['Merito'] = df['MERITO'].astype('str')
    df_resultado['EstadoPost'] = df['ESTADOPOST'].astype('str')
    df_resultado['Observacion'] = df['OBSERVACION'].astype('str')
    df_resultado['Resultado'] = df['RESULTADO'].astype('str')
    df_resultado['NombreColegio'] = df['NOMCOLEGI'].astype('str')
    df_resultado['PaisColegio'] = df['PAISCOL'].astype('str')
    df_resultado['UbigeoColegio'] = df['UBIGEOCOLE'].astype('str')
    df_resultado['DepartamentoColegio'] = df['DEPCOL'].astype('str')
    df_resultado['ProvinciaColegio'] = df['PROCOL'].astype('str')
    df_resultado['DistritoColegio'] = df['DISCOL'].astype('str')
    df_resultado['UbigeoColegio2'] = df['UBICOLEGI'].astype('str')
    df_resultado['DireccionColegio'] = df['DIRCOL'].astype('str')
    df_resultado['CodigoTipoColegio'] = df['CODTIPCO'].astype('str')
    df_resultado['TipoColegio'] = df['TIPCOLEGI'].astype('str')
    df_resultado['EgresoColegio'] = df['EGRESOCOL'].astype('str')
    df_resultado['CodigoPreparatoria'] = df['CODPREPAR'].astype('str')
    df_resultado['DescripcionPreparatoria'] = df['DESPREPAR'].astype('str')
    df_resultado['Departamento'] = df['DEPARTAMENTO'].astype('str')
    df_resultado['Provincia'] = df['PROVINCIA'].astype('str')
    df_resultado['Distrito'] = df['DISTRITO'].astype('str')
    df_resultado['UbigeoResidencia'] = df['UBIGEORESIDENCIA'].astype('str')
    df_resultado['CodigoEstadoCivil'] = df['CODESTCIV'].astype('str')
    df_resultado['EstadoCivil'] = df['ESTADOCIVI'].astype('str')
    df_resultado['Trabaja'] = df['TRABAJA'].astype('str')
    df_resultado['CodigoNivelEducacion'] = df['CODNIVEDU'].astype('str')
    df_resultado['DescripcionNivelEducacion'] = df['DESNIVEDU'].astype('str')
    df_resultado['TipoEnc'] = df['TIP_ENC'].astype('str')
    df_resultado['CodigoPais'] = df['CODPAIS'].astype('str')
    df_resultado['UniversidadProcedencia'] = df['UNIVERSIDADPROCEDENCIA'].astype('str')
    df_resultado['FacultadProcedencia'] = df['FACULTADPROCEDENCIA'].astype('str')
    df_resultado['EscuelaProcedencia'] = df['ESCUELAPROCEDENCIA'].astype('str')
    df_resultado['DesPais'] = df['DESPAIS'].astype('str')
    df_resultado['Grado1'] = df['GRADO1'].astype('str')
    df_resultado['Col1'] = df['COL1'].astype('str')
    df_resultado['Grado2'] = df['GRADO2'].astype('str')
    df_resultado['Col2'] = df['COL2'].astype('str')
    df_resultado['Grado3'] = df['GRADO3'].astype('str')
    df_resultado['Col3'] = df['COL3'].astype('str')
    df_resultado['ID'] = df['ID'].astype('str')
    
    print(df_resultado)
    
    return df_resultado


def load(df):
    
    # Creamos la conexion
    engine = sa.create_engine(DB_LOCATION)
    
    # Habilitar la conexion
    with engine.connect() as con:
        # Creamos la tabla
        sql_query = '''
            CREATE TABLE IF NOT EXISTS ProcesoAdmision(
                NombreAlumno VARCHAR(200), 
                CodSed VARCHAR(200), 
                SemApe VARCHAR(200), 
                CodEsp VARCHAR(200), 
                FechaExamen VARCHAR(200), 
                HoraExamen VARCHAR(200), 
                Sexo VARCHAR(200), 
                FechaNacimiento VARCHAR(200),
                DiaNacimiento VARCHAR(200),
                MesNacimiento VARCHAR(200),
                AnoNacimiento VARCHAR(200),
                CodigoModalidad VARCHAR(200),
                Modalidad VARCHAR(200),
                Sede VARCHAR(200),
                Semestre VARCHAR(200),
                Especialidad VARCHAR(200),
                CodFac VARCHAR(200),
                Facultad VARCHAR(200),
                Puntaje VARCHAR(200),
                Merito VARCHAR(200),
                EstadoPost VARCHAR(200),
                Observacion VARCHAR(200),
                Resultado VARCHAR(200),
                NombreColegio VARCHAR(200),
                PaisColegio VARCHAR(200),
                UbigeoColegio VARCHAR(200),
                DepartamentoColegio VARCHAR(200),
                ProvinciaColegio VARCHAR(200),
                DistritoColegio VARCHAR(200),
                UbigeoColegio2 VARCHAR(200),
                DireccionColegio VARCHAR(200),
                CodigoTipoColegio VARCHAR(200),
                TipoColegio VARCHAR(200),
                EgresoColegio VARCHAR(200),
                CodigoPreparatoria VARCHAR(200),
                DescripcionPreparatoria VARCHAR(200),
                Departamento VARCHAR(200),
                Provincia VARCHAR(200),
                Distrito VARCHAR(200),
                UbigeoResidencia VARCHAR(200),
                CodigoEstadoCivil VARCHAR(200),
                EstadoCivil VARCHAR(200),
                Trabaja VARCHAR(200),
                CodigoNivelEducacion VARCHAR(200),
                DescripcionNivelEducacion VARCHAR(200),
                TipoEnc VARCHAR(200),
                CodigoPais VARCHAR(200),
                UniversidadProcedencia VARCHAR(200),
                FacultadProcedencia VARCHAR(200),
                EscuelaProcedencia VARCHAR(200),
                DesPais VARCHAR(200),
                Grado1 VARCHAR(200),
                Col1 VARCHAR(200),
                Grado2 VARCHAR(200),
                Col2 VARCHAR(200),
                Grado3 VARCHAR(200),
                Col3 VARCHAR(200),
                ID VARCHAR(200)
            )
        '''
        sql_query = sa.sql.text(sql_query)
        con.execute(sql_query)
        print('===> Tabla creada satisfactoriamente')

    df.to_sql('ProcesoAdmision', engine, index=False, if_exists='append')

    print('===> Los datos fueron cargados satisfactoriamente')


if __name__ == '__main__':
    # Extraemos la informacion del ultimo proceso de admision
    df_admision = extract()

    # Transformamos Clientes y Deudas al formato solicitado
    df_alumnos_admision = transform(df_admision)

    # Load to DB
    load(df_alumnos_admision)

