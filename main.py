import pandas as pd
import os

import sqlalchemy as sa


FILENAME = 'file.ope'
INPUT_PATH = 'server_inputs/'
OUTPUT_PATH = 'server_outputs/'

DB_NAME = 'db_sbs.sqlite'
DB_LOCATION = f'sqlite:///database/{DB_NAME}'

COL_DEUDAS = {
        'CodigoSBS': 'Cod_SBS',
        'CodigoEmpresa': 'Cod_Emp',
        'TipoCredito': 'Tip_Credit',
        'ValorSaldo': 'Val_Saldo',
        'ClasificacionDeuda': 'Clasif_deu',
        'CodigoCuenta': 'Cod_Cuenta'
    }


def extract(ind_clientes):
    df_extracted = pd.read_csv(INPUT_PATH + FILENAME)
    df_extracted['Value'] = df_extracted['Field_1'].str[0]
    df_respuesta = df_extracted[df_extracted['Value'] == ('1' if ind_clientes else '2')].reset_index(drop=True).copy()
    return df_respuesta


def transform(df, ind_clientes):
    df_resultado = transform_clientes(df) if ind_clientes else transform_deudas(df)
    return df_resultado

def transform_clientes(df):
    df_clientes = pd.DataFrame()
    # Llenamos los campos de clientes
    df_clientes['SBSCodigoCliente'] = df['Field_1'].astype('str').str.split('|')[0]
    df_clientes['SBSFechaReporte'] = df['Field_1'].astype('str').str.split('|')[1]
    df_clientes['SBSTipoDocumentoT'] = df['Field_1'].astype('str').str.split('|')[2]
    df_clientes['SBSRucCliente'] = df['Field_1'].astype('str').str.split('|')[3]
    df_clientes['SBSTipoDocumento'] = df['Field_1'].astype('str').str.split('|')[4]
    df_clientes['SBSNumeroDocumento'] = df['Field_1'].astype('str').str.split('|')[5]
    df_clientes['SBSTipoPer'] = df['Field_1'].astype('str').str.split('|')[6]
    df_clientes['SBSTipoEmpresa'] = df['Field_1'].astype('str').str.split('|')[7]
    df_clientes['SBSNumeroEntidad'] = df['Field_1'].astype('str').str.split('|')[8]
    df_clientes['SBSSalNor'] = df['Field_1'].astype('str').str.split('|')[9]
    df_clientes['SBSSalCPP'] = df['Field_1'].astype('str').str.split('|')[10]
    df_clientes['SBSSalDEF'] = df['Field_1'].astype('str').str.split('|')[11]
    df_clientes['SBSSalDUD'] = df['Field_1'].astype('str').str.split('|')[12]
    df_clientes['SBSSalAPER'] = df['Field_1'].astype('str').str.split('|')[13]
    df_clientes['SBSAPEPAT'] = df['Field_1'].astype('str').str.split('|')[14]
    df_clientes['SBSAPEMAT'] = df['Field_1'].astype('str').str.split('|')[15]
    df_clientes['SBSAPECAS'] = df['Field_1'].astype('str').str.split('|')[16]
    df_clientes['SBSNOMCLI'] = df['Field_1'].astype('str').str.split('|')[17]
    df_clientes['SBSNOMCLI2'] = df['Field_1'].astype('str').str.split('|')[18]
    print(df_clientes)
    
    return df_clientes

def transform_deudas(df):
    df_deuda = pd.DataFrame()
    # Llenamos los campos de deudas
    df_deuda['CodigoSBS'] = df['Field_1'].str[1:11]
    df_deuda['CodigoEmpresa'] = df['Field_1'].str[11:16]
    df_deuda['TipoCredito'] = df['Field_1'].str[16:18]
    df_deuda['Nivel2'] = df['Field_1'].str[18:20]
    df_deuda['Moneda'] = df['Field_1'].str[20:21]
    df_deuda['SubCodigoCuenta'] = df['Field_1'].str[21:32]
    df_deuda['Condicion'] = df['Field_1'].str[32:38]
    df_deuda['ValorSaldo'] = df['Field_1'].str[38:42]
    df_deuda['ClasificacionDeuda'] = df['Field_1'].str[42:43]
    df_deuda['CodigoCuenta'] = df_deuda['Nivel2'] + df_deuda['Moneda'] + df_deuda['SubCodigoCuenta']
    
    # Renombramos los nombres de las columnas
    df_deuda.rename(columns=COL_DEUDAS, inplace=True)
    print(df_deuda)
    
    return df_deuda

def load(df_data, filename):
    print(f'===> Loading {filename}')
    path_output = os.path.join(OUTPUT_PATH, filename)
    df_data.to_csv(path_output)


def load_db(df):
    # Eliminamos la columna SubCodigoCuenta para que matche el df y la BD a definir
    df.drop('SubCodigoCuenta', axis=1, inplace=True)
    
    # Creamos la conexion
    engine = sa.create_engine(DB_LOCATION)
    
    # Habilitar la conexion
    with engine.connect() as con:
        # Creamos la tabla
        sql_query = '''
            CREATE TABLE IF NOT EXISTS deudas_sbs(
                Cod_SBS VARCHAR(200), 
                Cod_Emp VARCHAR(200), 
                Tip_Credit VARCHAR(200), 
                Nivel2 VARCHAR(200), 
                Moneda VARCHAR(200), 
                Condicion VARCHAR(200), 
                Val_Saldo VARCHAR(200), 
                Clasif_Deu VARCHAR(200), 
                Cod_Cuenta VARCHAR(200)
            )
        '''
        sql_query = sa.sql.text(sql_query)
        con.execute(sql_query)
        print('===> Tabla creada satisfactoriamente')

    df.to_sql('deudas_sbs', engine, index=False, if_exists='append')

    print('===> Los datos fueron cargados satisfactoriamente')
    


if __name__ == '__main__':
    # Extract Clientes y Deudas
    df_clientes = extract(True)
    df_deudas = extract(False)

    # Transformamos Clientes y Deudas al formato solicitado
    df_transformed_clientes = transform(df_clientes, True)
    df_transformed_deudas = transform(df_deudas, False)

    # Load
    load(df_transformed_clientes, 'clientes.csv')
    load(df_transformed_deudas, 'deudas.csv')

    # Load to DB
    load_db(df_transformed_deudas)

