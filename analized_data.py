import logging
import pandas as pd


logging.basicConfig(
    level=logging.DEBUG, format="%(levelname)s -> %(asctime)s: %(message)s"
)


def data_extraction():
    """
    Функция для извлечения данных из файлов 'direct_cost.xlsx', 'ads_direct.csv.csv'
    """
    try:
        data_xlsx = pd.read_excel(io="files/direct_cost.xlsx", index_col=0).iloc[
            :, [1, 2, 3, 4, 5]
        ]
        new_data = data_xlsx[(data_xlsx["Расход (руб.)"] > 0)]
        id_list = new_data.iloc[:, 0].to_list()
        cor_list = [int(i[2:]) for i in id_list]
        new_data.iloc[:, 0] = new_data.iloc[:, 0].replace(id_list, cor_list)

        data_csv = pd.read_csv(
            "files/ads_direct.csv .csv",
            encoding="utf-16",
            index_col=0,
            header=5,
            sep="\t",
            on_bad_lines="skip",
        ).iloc[:, [13, 14, 15, 16]]
        end_csv = data_csv[data_csv.iloc[:, 0].isin(cor_list)].drop_duplicates(
            keep="first"
        )
        return new_data, end_csv
    except Exception as ex:
        logging.exception(str(ex))


def create_new_data_files(dataframe):
    """
    Функция для создания новых файлов с отсортированными данными из файлов 'direct_cost.xlsx', 'ads_direct.csv.csv'
    :param dataframe: Фрейм данных полученных из функции data_extraction
    """
    try:
        df1 = pd.DataFrame(dataframe[1])
        headers = ["ad_id", "title_1", "title_2", "adv_text"]
        df1.to_csv(
            "new_files/new_ads_direct.csv",
            index=False,
            na_rep="Нет данных",
            header=headers,
        )

        df2 = pd.DataFrame(dataframe[0])
        headers = [
            "ad_id",
            "SUM(impressions)",
            "SUM(clicks)",
            "SUM(cost)",
            "SUM(conversion)",
        ]
        df2.to_csv(
            "new_files/new_direct_cost.csv",
            index=False,
            na_rep="Нет данных",
            header=headers,
        )
    except Exception as ex:
        logging.exception(str(ex))


def create_analyzed_data():
    """
    Функция для создания файла с проанализированными данными
    """
    try:
        new_direct_cost_file = pd.read_csv("new_files/new_direct_cost.csv", index_col=0)
        new_ads_direct_file = pd.read_csv("new_files/new_ads_direct.csv", index_col=0)
        result = new_ads_direct_file.merge(new_direct_cost_file, on="ad_id")
        result = result.sort_values(["SUM(cost)"], ascending=False)
        df3 = pd.DataFrame(result)
        headers = [
            "title_1",
            "title_2",
            "adv_text",
            "SUM(impressions)",
            "SUM(clicks)",
            "SUM(cost)",
            "SUM(conversion)",
        ]
        df3.to_csv(
            "new_files/analyzed_data.csv", index=1, na_rep="Нет данных", header=headers
        )
        logging.info(msg="Файл analyzed_data.csv с проанализированными данными создан!")
    except Exception as ex:
        logging.exception(str(ex))


def main():
    """
    Основная функция для вызова всех функций скрипта
    """
    dataframe = data_extraction()
    create_new_data_files(dataframe)
    create_analyzed_data()


if __name__ == "__main__":
    main()
