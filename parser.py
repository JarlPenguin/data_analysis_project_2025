import pandas as pd

def get_df(sec_id, start_date="2020-01-01", end_date="2024-10-31", local=False, save_to_file=False) -> pd.DataFrame:
    """
    Получает исторические данные по ценной бумаге с Московской биржи (MOEX) и возвращает их в виде DataFrame.

    Параметры:
    -----------
    sec_id : str
        Идентификатор ценной бумаги (например, тикер акции).

    start_date : str, optional, default="2020-01-01"
        Начальная дата периода, за который запрашиваются данные, в формате "YYYY-MM-DD".

    end_date : str, optional, default="2024-10-31"
        Конечная дата периода, за который запрашиваются данные, в формате "YYYY-MM-DD".

    local : bool, optional, default=False
        Если True, данные загружаются из локального CSV-файла, расположенного в папке data.
        Если False, данные загружаются с сайта Московской биржи.

    save_to_file : bool, optional, default=False
        Если True, загруженные данные сохраняются в локальный CSV-файл в папке data.
        Игнорируется, если local=True.

    Возвращает:
    -----------
    pd.DataFrame
        DataFrame с историческими данными по ценной бумаге.
    """

    if local:
        df = pd.read_csv(f"data/{sec_id}.csv")
    else:
        dfs = []
        url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/tqbr/securities/{sec_id}/candles.html?from={start_date}&till={end_date}"
        retrieved = 0
        while True:
            df, cursor = pd.read_html(url + f"&start={retrieved}")
            dfs.append(df)
            retrieved += cursor.at[0, "PAGESIZE (int64)"]
            total = cursor.at[0, "TOTAL (int64)"]
            if retrieved >= total:
                break

        df = pd.concat(dfs, axis=0, ignore_index=True)
        if save_to_file:
            df.to_csv(f"data/{sec_id}.csv", index=False)
    return df