PASOS PARA CONFIGURAR EL PROYECTO

1.      En la raiz del proyecto crear el entorno virtual ejecutando el comando: virtualenv env
2.      Para activar el entorno creado, en un terminal bash ejecutamos el siguiente comando: source env/bin/activate(Mac)
3.      Necesitamos instalar la librerias pandas y sqlalchemy: 
3.1     Dentro del entorno virtual, ejecutar el siguiente comando: pip install pandas
3.2     Dentro del entorno virtual, ejecutar el siguiente comando: pip install sqlalchemy
4.      Antes de ejecutar el proyecto por primera vez, necesitamos crear la siguiente estrutura de carpetas en la raiz del proyecto:
4.1     server_inputs: donde alojaremos el archivo de entrada que procesaremos
4.2     server_outputs: donde alojaremos los archivos de salida del proyecto: clientes.csv y deudas.csv
4.3     database: donde se creara automaticamente la base de datos con la data de deudas previamente procesada.

IMPORTANTE: Debemos asegurarnos que el Python Interpreter con el que estamos trabajando, sea el del virtualenv que hemos creado.
