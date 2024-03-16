import pandas as pd
from sqlalchemy import create_engine
from analized_data import logging


engine = create_engine("postgresql+psycopg2://userdb:passdb@localhost/testdb")


def create_db():
    """
    Функция для создания таблиц в базе данных postgresql с данными из файлов direct_cost.xlsx, ads_direct.csv .csv.
    """
    try:
        data_xlsx = pd.read_excel(io="files/direct_cost.xlsx", index_col=0).iloc[
            :, [1, 2, 3, 4, 5]
        ]
        data_xlsx.rename(
            columns={
                "№ Объявления": "ad_id",
                "Показы": "sum_impressions",
                "Клики": "sum_clicks",
                "Расход (руб.)": "sum_cost",
                "Конверсии": "sum_conversion",
            },
            inplace=True,
        )
        id_list = data_xlsx.iloc[:, 0].to_list()
        cor_list = [int(i[2:]) for i in id_list]
        data_xlsx.iloc[:, 0] = data_xlsx.iloc[:, 0].replace(id_list, cor_list)
        data_xlsx.to_sql("direct_cost", engine, index=False, if_exists="replace")

        data_csv = pd.read_csv(
            "files/ads_direct.csv .csv",
            encoding="utf-16",
            index_col=0,
            header=2,
            sep="\t",
            on_bad_lines="skip",
        ).iloc[:, [13, 14, 15, 16]]
        data_csv.rename(
            columns={
                "ID объявления": "ad_id",
                "Заголовок 1": "title_1",
                "Заголовок 2": "title_2",
                "Текст": "adv_text",
            },
            inplace=True,
        )
        data_csv.to_sql("ads_direct", engine, index=False, if_exists="replace")
        logging.info("Таблцы direct_cost, ads_direct в БД успешно созданы.")
    except Exception as ex:
        logging.exception(str(ex))


def get_analized_data():
    """
    Функция для получения  проанализированных данных из БД с использованием SQL-запроса.
    """
    try:
        with engine.connect() as connection:
            aaa = pd.read_sql_query(
                """
        SELECT DISTINCT ad.ad_id, ad.title_1, ad.title_2, ad.adv_text,
        dc.sum_impressions, dc.sum_clicks, dc.sum_cost, dc.sum_conversion
        FROM ads_direct ad JOIN direct_cost dc
        ON ad.ad_id = dc.ad_id AND dc.sum_cost > 0
        ORDER BY dc.sum_cost DESC
        """,
                connection,
            )

            aaa.to_csv("new_files/analyzed_sql_data.csv", index=0, na_rep="Нет данных")
            logging.info(
                msg="Файл analyzed_sql_data.csv с проанализированными данными создан!"
            )
    except Exception as ex:
        logging.exception(str(ex))


def main():
    """
    Основная функция для вызова всех функций скрипта
    """
    create_db()
    get_analized_data()


if __name__ == "__main__":
    main()
