final project

1) Источником данных служит S3 хранилище. Работа выполняется одним дагом final_dag.py
2) В бакетах могут быть разные даты, поэтому я их загружаю инкрементально, обрабатывая с помощью pandas и сохраняю каждый файл бакета в отдельную папку с датой в названии
Пример директории: /data/2022-10-01/batch.csv Файл s3_2_csv.py
3) Следующая таска выгружает из соответствующей папки файл бакета в vertica слой stg. Файл csv_2_stg.py
4) В запросе выгрузки данных в cdm слой данные фильтруются: убираются аккаунты со значением -1, status = done. Файл stg_2_cdm.py

Скриншот витрины pic\Screenshot.png