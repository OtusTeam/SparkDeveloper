### Как запустить Spark в Docker

`docker run --name otus-spark -v {ПУТЬ_К_ФАЙЛАМ_JSON}:/home/jovyan/data -it apache/spark /opt/spark/bin/spark-shell`

Замените `{ПУТЬ_К_ФАЙЛАМ_JSON}` полным путем к папке, где лежат файлы `customer_data.json` и `retail_data.json`