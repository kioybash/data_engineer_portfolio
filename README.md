# My Data Engineer Portfolio
📌 **Здесь представлены проекты, выполненные в процессе обучения.**

------------

 ## 🔵Проектирование DWH
**Цель:** Провести анализ предметной области и разработать хранилище данных с использованием трех методологий: Dimensional modeling, Data Vault, Anchor Modeling.

**Результат:**
1. [**Описание предметной области**](https://github.com/kioybash/data_engineer_portfolio/blob/main/dwh_design/%D0%9F%D1%80%D0%B5%D0%B4%D0%BC%D0%B5%D1%82%D0%BD%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C%20%D0%A0%D0%B0%D1%81%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%B3%D1%80%D0%B0%D0%BD%D1%82%D0%BE%D0%B2%20%D0%B2%20%D0%A4%D0%BE%D0%BD%D0%B4%D0%B5.pdf "**Описание предметной области**")
2. ER-диаграммы: [**Dimensional modeling**](https://github.com/kioybash/data_engineer_portfolio/blob/main/dwh_design/Dimensional%20modeling.jpg "**Dimensional modeling**"), [**Data Vault**](https://github.com/kioybash/data_engineer_portfolio/blob/main/dwh_design/Data%20Vaulth.jpg "**Data Vault**"), [**Anchor Modeling**](https://github.com/kioybash/data_engineer_portfolio/blob/main/dwh_design/Anchor%20Modeling.png "Anchor Modeling")

------------

## 🔵Создание витрины данных (Hadoop. YARN, MapReduce)

**Задача:** В облачном кластере скопировать открытый датасет [NYC Yellow Taxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page "NYC Yellow Taxi") в свой S3-бакет и написать приложение MapReduce, вычисляющее отчет для каждого месяца 2020 года с тремя колонками: Payment type, Month, Tips average amount.

**Результат:** [**Приложение MapReduce**](https://github.com/kioybash/data_engineer_portfolio/tree/main/mapreduce "**Приложение MapReduce**")

**Пояснение:** Задача выполнена в облачном кластере Yandex.Cloud.

------------

## 🔵Создание витрины данных (Spark)

**Задача:** В облачном кластере скопировать открытый датасет [NYC Yellow Taxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page "NYC Yellow Taxi") в свой S3-бакет и написать Spark-приложение, вычисляющее отчет для каждого месяца 2020 года с четырьмя колонками: Payment type, Month, Average trip cost, Avg trip km cost.

**Результат:** [**Spark-приложение**](https://github.com/kioybash/data_engineer_portfolio/blob/main/spark_job_datamart_nytaxi.py "**Spark-приложение**")

**Пояснение:** Задача выполнена в облачном кластере Yandex.Cloud.

------------

## 🔵Создание DAG и Operator (Airflow)

**Задача #1:** Задача заключается в автоматизации процесса извлечения данных о курсах валют с официального сайта [**Банк России**](https://github.com/kioybash/data_engineer_portfolio/blob/main/spark_job_datamart_nytaxi.py "**Банк России**") и их последующем хранении в базе данных GreenPlum. 

**Результат:** [**DAG**](https://github.com/kioybash/data_engineer_portfolio/blob/main/airflow/v-kiojbash_cbr.py "**DAG**")

**Задача #2:** Создать DAG, который работает с понедельника по субботу (кроме воскресенья), обращается к GreenPlum, извлекает значение heading из строки с id, равным дню недели, и сохраняет его в XCom.

**Результат:** [**DAG**](https://github.com/kioybash/data_engineer_portfolio/blob/main/airflow/v-kiojbash_lesson4.py "**DAG**")

**Задача #3:** Создать DAG, который создает таблицу в GreenPlum с заданными полями, используя API (https://rickandmortyapi.com/documentation/#location) для поиска трех локаций сериала "Рик и Морти" с наибольшим числом резидентов и записывает значения в таблицу.

**Результат:** [**DAG**](https://github.com/kioybash/data_engineer_portfolio/blob/main/airflow/v-kiojbash_lesson5.py "**DAG**")

------------

## 🔵Обучение модели для банковского скоринга и оценки заемщика (SparkML)

**Задача #1:** Реализовать пропущенную логику обучения модели для оценки благонадежности клиентов банка, используя шаблон. Использовать метрику accuracy, привести булевский тип данных к числовому формату.

**Результат:** [**Spark-приложение**](https://github.com/kioybash/data_engineer_portfolio/blob/main/spark_ml/bank_scoring.py "**Spark-приложение**")

**Задача #2:** Реализовать пропущенную логику обучения модели для прогнозирования максимальной суммы кредита, которую банк может предложить клиенту, на основе данных о клиенте. Для оценки использовать метрику RMSE.

**Результат:** [**Spark-приложение**](https://github.com/kioybash/data_engineer_portfolio/blob/main/spark_ml/bank_credit_count.py "**Spark-приложение**")

**Пояснение:** Шаблон предоставляет основу, к которой я добавил код в соответствующих функциях.

------------

## 🔵Логирование эксперимента (MLFlow)

**Задача:** Разметить эксперимент для трекинга процесса обучения модели в MLFlow. В разметку включены 4 метрики, 10 параметров и залогированная модель.

**Результат:** [**Обучение модели**](https://github.com/kioybash/data_engineer_portfolio/blob/main/mlflow/PySparkFit.py "**Обучение модели**"),  [**Применение модели**](https://github.com/kioybash/data_engineer_portfolio/blob/main/mlflow/PySparkPredict.py "**Применение модели**")

------------


## 🔵Развернуть Spark в K8S (Cloud Solutions)

**Задача:** 
1. Установить Spark Operator в кластере Kubernetes.
2. С использованием Spark Operator запустить тестовое приложение для расчета числа Пи.
3. Запустить собственное приложение, которое считывает данные из S3 и перемещает их в другой бакет в S3.
4. Установить Spark History Server в Kubernetes.
5. Запустить приложение Spark, записывающее логи в Spark History Server.

**Результат:** Выполнено, но результаты не предоставлены.

**Дополнительно:** Также в облачном Kubernetes развернут JupyterHub и MLFlow.

------------
