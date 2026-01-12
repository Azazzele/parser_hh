import asyncio
import aiohttp
import pandas as pd
from typing import List, Dict, Optional
import sys
import io
import os

# ФИКС WINDOWS КОНСОЛИ (обязательно!)
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

HH_API_URL = "https://api.hh.ru/vacancies"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "X-User-Agent": "HH-User/8.0 (Android 14; Pixel 8; 1080x2400; 8.0.1; 98765432)",
    "Referer": "https://hh.ru/",
}

async def fetch_vacancies(session: aiohttp.ClientSession, page: int = 0, per_page: int = 100, text: str = "Python developer") -> List[Dict]:
    params = {
        "text": text,
        "page": page,
        "per_page": per_page,
        "only_with_salary": "0",
        "area": 1,
    }
    async with session.get(HH_API_URL, params=params, headers=DEFAULT_HEADERS) as response:
        if response.status != 200:
            print(f"Поиск вакансий: {response.status}")
            return []
        data = await response.json()
        return data.get("items", [])

async def get_vacancy_details(session: aiohttp.ClientSession, vacancy_id: str) -> Optional[Dict]:
    url = f"https://api.hh.ru/vacancies/{vacancy_id}"
    async with session.get(url, headers=DEFAULT_HEADERS) as response:
        if response.status == 403:
            print(f"🔒 403 на {vacancy_id} - API закрыт, нужен токен!")
            return None
        if response.status != 200:
            print(f"Детали {vacancy_id}: {response.status}")
            return None
        return await response.json()

def collect_skills_data(vacancies: List[Dict], details_list: List[Optional[Dict]]) -> tuple[List[Dict], List[Dict]]:
    detailed_rows = []
    compact_rows = []
    successful_details = 0
    
    for i, (vacancy, details) in enumerate(zip(vacancies, details_list)):
        vacancy_id = vacancy['id']
        title = vacancy.get('name', 'Без названия')
        company = vacancy.get('employer', {}).get('name', 'Неизвестно')
        url = f"https://hh.ru/vacancy/{vacancy_id}"
        
        if details and 'key_skills' in details:
            successful_details += 1
            key_skills = details['key_skills']
            skill_names = [skill.get('name', '').strip() for skill in key_skills if skill.get('name')]
            
            print(f"{vacancy_id}: {len(skill_names)} скиллов - {', '.join(skill_names[:3])}...")
            
           
            for skill in skill_names:
                detailed_rows.append({
                    'Вакансия': title[:100],
                    'Компания': company,
                    'ID': vacancy_id,
                    'URL': url,
                    'Навык': skill,
                    'Уровень': skill in skill_names[:3] and 'Высокий' or 'Доп.',
                })
            
            
            compact_rows.append({
                'Вакансия': title[:100],
                'Компания': company,
                'ID': vacancy_id,
                'URL': url,
                'Навыки': ', '.join(skill_names),
                'Количество': len(skill_names)
            })
        else:
            print(f"❌ {vacancy_id}: нет деталей (вероятно 403)")
           
    
    print(f"\n СТАТИСТИКА: {successful_details}/{len(vacancies)} вакансий с деталями")
    return detailed_rows, compact_rows

async def main():
    connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        print(" Собираем Python-вакансии...")
        vacancies = await fetch_vacancies(session, page=0, per_page=100)
        
        if not vacancies:
            print(" Вакансий нет")
            return
        
        print(f" Найдено {len(vacancies)} вакансий")
        
        # Параллельно детали (с защитой от бана)
        tasks = [get_vacancy_details(session, v["id"]) for v in vacancies[:50]]  # первые 50
        details_results = await asyncio.gather(*tasks, return_exceptions=True)
        details_list = [d for d in details_results if not isinstance(d, Exception) and d]
        
        detailed_rows, compact_rows = collect_skills_data(vacancies[:50], details_results)
        
        if not detailed_rows:
            print(" НИ ОДНОЙ вакансии с навыками! API мёртв - переходим на HTML парсинг")
            return
        
     
        df_detailed = pd.DataFrame(detailed_rows)
        df_compact = pd.DataFrame(compact_rows)
        
        filename = "python_skills_FULL.xlsx"
        full_path = os.path.abspath(filename)
        
        print(f"\n СОХРАНЯЮ EXCEL СЮДА: {full_path}")
        print(f" Навыков найдено: {len(df_detailed)}")
        print(f" Уникальных скиллов: {df_detailed['Навык'].nunique()}")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_detailed.sort_values('Навык').to_excel(writer, sheet_name='Все навыки', index=False)
            df_compact.sort_values('Количество', ascending=False).to_excel(writer, sheet_name='Топ вакансии', index=False)
            
            top_skills = df_detailed['Навык'].value_counts().head(20).reset_index()
            top_skills.columns = ['Навык', 'Количество вакансий']
            top_skills.to_excel(writer, sheet_name='ТОП-20 скиллов', index=False)
        
        print(" EXCEL ГОТОВ! 3 листа:")
        print("  1. 'Все навыки' - по строке на скилл")
        print("  2. 'Топ вакансии' - вакансии с кучей скиллов")
        print("  3. 'ТОП-20 скиллов' - рейтинг!")
        

        print("\n ТОП-10 СКИЛЛОВ:")
        print(df_detailed['Навык'].value_counts().head(10))

if __name__ == "__main__":
    asyncio.run(main())