from tabulate import tabulate
import os
import urllib.request
import pandas as pd
from datetime import datetime

NOAA_TO_UA = {  # Мапа індексів NOAA 
    1: "Cherkasy", 2: "Chernihiv", 3: "Chernivtsi", 4: "Chernivtsi",
    5: "Dnipropetrovs'k", 6: "Donets'k", 7: "Ivano-Frankivs'k", 8: "Kharkiv`",
    9: "Kherson", 10: "Khmel'nyts'kyy", 11: "Kyiv", 12: "Kyiv City",
    13: "Kirovohrad", 14: "Luhans'k", 15: "L'viv", 16: "Mykolayiv",
    17: "Odessa", 18: "Poltava", 19: "Rivne", 20: "Sevastopol",
    21: "Sumy", 22: "Ternopil", 23: "Transcarpathia", 24: "Vinnytsya",
    25: "Volyn", 26: "Zaporizhzhya", 27: "Zhytomyr"
}

DATA_DIR = "vhi_data"
URL_TEMPLATE = "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_TS_admin.php?country=UKR&provinceID={}&year1=1981&year2=2024&type=Mean"

os.makedirs(DATA_DIR, exist_ok=True)

def download_vhi(province_id):
    """Завантажує VHI-файл для області"""
    url = URL_TEMPLATE.format(province_id)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DATA_DIR}/VHI_{NOAA_TO_UA[province_id]}_{timestamp}.csv"
    
    if not any(NOAA_TO_UA[province_id] in f for f in os.listdir(DATA_DIR)):
        urllib.request.urlretrieve(url, filename)
        print(f"Завантажено: {filename}")
    else:
        print(f"Пропущено: {NOAA_TO_UA[province_id]} (вже є дані)")

def load_data(directory):
    """Зчитує всі файли в `DataFrame`"""
    all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]
    df_list = []

    for file in all_files:
        province = file.split("_")[2]
        df = pd.read_csv(file, index_col=False, header=1)
        df.columns = ["year", "week", "SMN", "SMT", "VCI", "TCI", "VHI"]
        df["region"] = province
        df = df[pd.to_numeric(df["VHI"], errors='coerce').notna()]
        df = df[df['year'].astype(str).str.match(r'\d{4}')]  # Видаляємо рядки з некоректним роком
        
        for key, val in NOAA_TO_UA.items():
            if val == province:
                df["id"] = key  
                break
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True)


def vhi_med_min_max(df):
    return(df.groupby(['region', 'year']).agg({'VHI': ['mean', 'min', 'max']}))

def vhi_for_year(df, year):
    return(df[['id','region', 'VHI', 'year', 'week', ]].loc[df['year'] == year])

def vhi_for_couple(df, years):
    return(df[df['year'].isin(years)][['id', 'region', 'VHI', 'year', 'week']])

def vhi_drought(df):
    extreme_drought = df[(df['VHI'] <= 15) & (df['VHI'] > -1)]
    drought_years = extreme_drought.groupby('year')['region'].nunique()
    critical_years = drought_years[drought_years > 27 * 0.2].index

    result = (
        extreme_drought[extreme_drought['year'].isin(critical_years)]
        .groupby(['year', 'region'], as_index=False)['VHI']
        .mean()
    )
    return(result)

# Завантаження файлів
for region_id in NOAA_TO_UA:
    download_vhi(region_id)

df = load_data(DATA_DIR)

test1 = vhi_med_min_max(df)
test2 = vhi_for_year(df, '2000')
test3 = vhi_for_couple(df, ['2000', '2001', '2002'])
test4 = vhi_drought(df)

print(tabulate(test1, headers = 'keys', tablefmt = 'psql'))
print(tabulate(test2, headers = 'keys', tablefmt = 'psql'))
print(tabulate(test3, headers = 'keys', tablefmt = 'psql'))
print(tabulate(test4, headers = 'keys', tablefmt = 'psql'))
