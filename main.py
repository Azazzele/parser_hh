import asyncio
import aiohttp
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict, Tuple, Optional
import sys
import io
import os
import time
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

# Фикс кодировки Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

HH_API_URL = "https://api.hh.ru/vacancies"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": "https://hh.ru/",
}

# Настройки поиска — меняйте здесь
SEARCH_TEXT = "Python developer OR Python разработчик"
AREA = 1          # 1 = Москва, 113 = Россия и др.
DAYS_AGO = 30
MAX_PAGES = 20

async def fetch_vacancies(
    session: aiohttp.ClientSession,
    page: int,
    per_page: int = 100
) -> List[Dict]:
    date_from = (datetime.utcnow() - timedelta(days=DAYS_AGO)).strftime("%Y-%m-%d")
    
    params = {
        "text": SEARCH_TEXT,
        "page": page,
        "per_page": per_page,
        "area": AREA,
        "date_from": date_from,
        "only_with_salary": "0",
    }
    
    for attempt in range(3):
        try:
            async with session.get(HH_API_URL, params=params, headers=DEFAULT_HEADERS, timeout=20) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("items", [])
                elif resp.status == 429:
                    print(f"429 Too Many Requests (стр {page}) → ждём 10 сек")
                    await asyncio.sleep(10)
                else:
                    print(f"Статус {resp.status} на странице {page}")
                    return []
        except Exception as e:
            print(f"Ошибка стр {page}, попытка {attempt+1}: {e}")
            await asyncio.sleep(3)
    
    return []


def parse_salary(salary: Optional[Dict]) -> Dict:
    if not salary:
        return {"from": None, "to": None, "currency": None, "gross": None}
    return {
        "from": salary.get("from"),
        "to": salary.get("to"),
        "currency": salary.get("currency"),
        "gross": salary.get("gross", False)
    }


def collect_data(vacancies: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    detailed_rows = []
    summary_rows = []

    for vacancy in vacancies:
        vid = vacancy.get("id")
        if not vid:
            continue

        title = vacancy.get("name", "Без названия")
        company = vacancy.get("employer", {}).get("name", "Неизвестно")
        url = f"https://hh.ru/vacancy/{vid}"
        published = vacancy.get("published_at", "")[:10]

        salary = parse_salary(vacancy.get("salary"))

        skills = vacancy.get("key_skills", [])
        skill_names = [s["name"].strip() for s in skills if s.get("name") and s["name"].strip()]

        if skill_names:
            for skill in skill_names:
                detailed_rows.append({
                    "Дата": published,
                    "Вакансия": title[:120],
                    "Компания": company[:80],
                    "ID": vid,
                    "URL": url,
                    "Навык": skill,
                    "ЗП от": salary["from"],
                    "ЗП до": salary["to"],
                    "Валюта": salary["currency"],
                })

            summary_rows.append({
                "Дата": published,
                "Вакансия": title[:120],
                "Компания": company[:80],
                "ID": vid,
                "URL": url,
                "Навыки": ", ".join(skill_names),
                "Кол-во навыков": len(skill_names),
                "ЗП от": salary["from"],
                "ЗП до": salary["to"],
                "Валюта": salary["currency"],
            })

    return pd.DataFrame(detailed_rows), pd.DataFrame(summary_rows)


def save_visualization(df: pd.DataFrame, filename: str):
    if df.empty:
        print("Нет данных для графика")
        return

    top_skills = df["Навык"].value_counts().head(15)
    
    plt.figure(figsize=(12, 7))
    top_skills.plot(kind="barh", color="#1f77b4")
    plt.title(f"Топ-15 навыков • {SEARCH_TEXT} • Москва • последние {DAYS_AGO} дн.")
    plt.xlabel("Количество упоминаний")
    plt.ylabel("Навык")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(filename, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"График сохранён → {filename}")


async def main():
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Скрипт запущен из директории: {script_dir}")
    print(f"Текущая рабочая директория: {os.getcwd()}\n")

    connector = aiohttp.TCPConnector(limit=15, limit_per_host=4)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        print(f"Поиск: {SEARCH_TEXT}")
        print(f"Регион: {'Москва' if AREA == 1 else 'Вся Россия'}")
        print(f"Период: последние {DAYS_AGO} дней\n")

        all_vacancies: List[Dict] = []
        page = 0

        while page < MAX_PAGES:
            vacancies = await fetch_vacancies(session, page)
            if not vacancies:
                print(f"Страница {page} пустая → завершаем сбор")
                break

            all_vacancies.extend(vacancies)
            print(f"Страница {page:2d}: +{len(vacancies):3d}  (всего: {len(all_vacancies):4d})")
            
            page += 1
            await asyncio.sleep(1.6 + (page % 5) * 0.4)

        if not all_vacancies:
            print("Не удалось собрать ни одной вакансии")
            return

        print(f"\nВсего собрано вакансий: {len(all_vacancies)}")

        df_detailed, df_summary = collect_data(all_vacancies)

        if df_detailed.empty:
            print("Навыки не найдены в preview-ответах API")
            print("Рекомендация: получить OAuth-токен для детальных запросов → https://dev.hh.ru/")
            return

        # Формируем имена файлов
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = os.path.join(script_dir, f"hh_python_vacancies_{timestamp}.xlsx")
        plot_file  = os.path.join(script_dir, f"top_skills_{timestamp}.png")

        print(f"\nСохраняю файлы в директорию скрипта:")
        print(f"  • Excel  → {excel_file}")
        print(f"  • График → {plot_file}\n")

        print(f"Всего строк с навыками : {len(df_detailed):,}")
        print(f"Уникальных навыков     : {df_detailed['Навык'].nunique():,}\n")

        with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
            df_detailed.sort_values(["Навык", "Дата"]).to_excel(writer, sheet_name="Все навыки", index=False)
            df_summary.sort_values("Кол-во навыков", ascending=False).to_excel(writer, sheet_name="Вакансии", index=False)

            top_skills = df_detailed["Навык"].value_counts().head(25).reset_index()
            top_skills.columns = ["Навык", "Упоминаний"]
            top_skills.to_excel(writer, sheet_name="ТОП-25 навыков", index=False)

            rub = df_detailed[df_detailed["Валюта"] == "RUR"].copy()
            if not rub.empty:
                skill_salary = rub.groupby("Навык")[["ЗП от", "ЗП до"]].median().reset_index()
                skill_salary["Средняя вилка"] = (skill_salary["ЗП от"] + skill_salary["ЗП до"]) / 2
                skill_salary.sort_values("Средняя вилка", ascending=False).to_excel(writer, sheet_name="ЗП по навыкам", index=False)

        save_visualization(df_detailed, plot_file)

        print("\nТОП-15 навыков:")
        print(df_detailed["Навык"].value_counts().head(15))

        print("\nГотово!")
        print("Файлы сохранены рядом со скриптом (.py файлом).")
        print("Проект готов для портфолио → добавьте README.md с описанием и скриншотами!")


if __name__ == "__main__":
    asyncio.run(main())
