import scrapy
import psycopg2
from scrapy.exceptions import CloseSpider
import re
import os
from dotenv import load_dotenv

load_dotenv()

class VavscrpSpider(scrapy.Spider):
    name = "vavscrp"
    allowed_domains = ["vavsynergy.com"]
    start_urls = ["https://vavsynergy.com/job/?jobs_ppp=-1m"]

    def __init__(self):
        # Подключение к базе данных
        self.conn = psycopg2.connect(
            dbname=os.getenv("NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
        )
        self.cursor = self.conn.cursor()
        self.existing_vacancies = self.get_existing_vacancies()

    def get_existing_vacancies(self):
        """Получает список всех существующих vac_id в базе."""
        self.cursor.execute("SELECT vac_id FROM vac_form_vacancy")
        return {row[0] for row in self.cursor.fetchall()}

    def parse(self, response):
        links = response.css('.job-title a::attr(href)').getall()
        current_vacancies = set()

        for link in links:
            vac_id = link.split('ua')[-1].replace('/', '').replace('-', '')
            current_vacancies.add(vac_id)

            if vac_id not in self.existing_vacancies:
                yield scrapy.Request(link, callback=self.parse_job, meta={"vac_id": vac_id})

        # Обновляем вакансии, которых больше нет на сайте
        inactive_vacancies = self.existing_vacancies - current_vacancies
        if inactive_vacancies:
            print(f"Удалена вакансія: {inactive_vacancies}")
            self.cursor.executemany(
                "DELETE FROM vac_form_vacancy WHERE vac_id = %s",
                [(vac_id,) for vac_id in inactive_vacancies]
            )
            self.conn.commit()

    def parse_job(self, response):
        vac_id = response.meta["vac_id"]
        position = response.css('.job-detail-title::text').get()
        job_category = response.css('.job-category a::text').get()
        country = response.css('.job-location a::text').get()
        salary_numbers = response.css('.job-salary .price-text::text').get()
        salary_currency = response.css('.job-salary .suffix::text').get()
        salary_time = response.css('.job-salary::text').get().replace(" ", "")
        salary = f"{salary_numbers} {salary_currency} {salary_time}"
        date_posted = response.css('.list li')[0].css('.details .value::text').get().strip()
        sex = response.css('.list li')[-2].css('.details .value::text').get().strip()
        vaccity = response.css('.vaccityua .content::text').get()
        docs_need = response.css('.docs-need-ua .content::text').get()
        schedule = response.css('.schedule-ua .content::text').get()
        apartment = response.css('.apartmentua .content::text').get()
        uniform = response.css('.uniform-ua .content::text').get()
        tools = response.css('.tool-ua .content::text').get()
        transfer = response.css('.transfer-ua .content::text').get()
        
        age = response.css('.ageua .content::text').get()
        try:
            min_age, max_age = re.findall(r'\d+', age)
        except:
            min_age = 18
            max_age = re.findall(r'\d+', age)[0]

        experience = response.css('.expirience-ua .content::text').get()
        language = response.css('.language-ua .content::text').get()
        duties = response.css('.duties-ua .content p::text').get()
        payment = response.css('.payment-ua .content::text').get()
        active = True

        # Сохранение данных в базу
        self.cursor.execute(
            """
            INSERT INTO vac_form_vacancy (
                vac_id, position, job_category, country, salary, date_posted, sex, 
                vaccity, docs_need, schedule, apartment, uniform, tools, transfer, age,
                min_age, max_age, experience, language, duties, payment, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vac_id) DO UPDATE SET 
                position = EXCLUDED.position,
                job_category = EXCLUDED.job_category,
                country = EXCLUDED.country,
                salary = EXCLUDED.salary,
                date_posted = EXCLUDED.date_posted,
                sex = EXCLUDED.sex,
                vaccity = EXCLUDED.vaccity,
                docs_need = EXCLUDED.docs_need,
                schedule = EXCLUDED.schedule,
                apartment = EXCLUDED.apartment,
                uniform = EXCLUDED.uniform,
                tools = EXCLUDED.tools,
                transfer = EXCLUDED.transfer,
                age = EXCLUDED.min_age,
                min_age = EXCLUDED.min_age,
                max_age = EXCLUDED.max_age,
                experience = EXCLUDED.experience,
                language = EXCLUDED.language,
                duties = EXCLUDED.duties,
                payment = EXCLUDED.payment,
                active = EXCLUDED.active
            """,
            (vac_id, position, job_category, country, salary, date_posted, sex, vaccity,
             docs_need, schedule, apartment, uniform, tools, transfer, age, min_age, max_age, experience,
             language, duties, payment, active)
        )
        self.conn.commit()

    def closed(self, reason):
        """Закрывает подключение к базе данных при завершении работы паука."""
        self.cursor.close()
        self.conn.close()
