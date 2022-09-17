import csv
import datetime
import json

import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import date
from sqlalchemy import Column, String, Integer
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.orm import sessionmaker, declarative_base, Session

house_data = []

Base = declarative_base()


class Apartments(Base):
    __tablename__ = 'Apartments'

    id = Column(Integer, primary_key=True)
    picture = Column(String)
    title = Column(String)
    date_publish = Column(String)
    city = Column(String)
    bedroom = Column(String)
    description = Column(String)
    price = Column(String)


async def get_task_data(session, page):
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Mobile Safari/537.36"
    }
    url = f'https://www.kijiji.ca/b-apartments-condos/city-of-toronto/page-{page}/c37l1700273'

    async with session.get(url=url, headers=headers) as response:
        main_text = await response.text()

        soup = BeautifulSoup(main_text, "lxml")

        answers = soup.find_all('div', class_='clearfix')
        for answer in answers:
            try:
                picture = answer.find('img')['data-src']
            except:
                picture = 'no'

            try:
                title = answer.find('div', class_='info').find('div', class_="info-container") \
                    .find('div', class_='title').find('a').text.replace('\n', '').strip()
            except:
                title = 'no'

            try:
                date_dirty = answer.find('div', class_='info').find('div', class_="info-container") \
                    .find('div', class_='location').find('span', class_='date-posted').text
                if len(date_dirty) == 10:
                    date_numbers = date_dirty.split('/')
                    date_publish = date_numbers[0] + '-' + date_numbers[1] + '-' + date_numbers[2]
                elif len(date_dirty) < 10:
                    today = date.today()
                    day = today.strftime("%d")
                    month_and_year = today.strftime("-%m-%Y")
                    date_publish = str(int(day) - 1) + month_and_year
                else:
                    today = date.today()
                    date_publish = today.strftime("%d-%m-%Y")
            except:
                date_publish = 'no'

            try:
                city = answer.find('div', class_='info').find('div', class_="info-container") \
                    .find('div', class_='location').find('span').text.strip()
            except:
                city = 'no'

            try:
                bedroom_dirty = answer.find('div', class_='rental-info').find('span', class_="bedrooms") \
                    .text.replace(' ', '').rstrip('\n')
                bedroom = bedroom_dirty[8:]
            except:
                bedroom = 'no'

            try:
                description = answer.find('div', class_='info').find('div', class_="info-container") \
                    .find('div', class_='description').text.replace('\n', '').strip()
            except:
                description = 'no'

            try:
                price = answer.find('div', class_='info').find('div', class_="info-container") \
                    .find('div', class_='price').text.strip()
            except:
                price = 'no'

            house_data.append(
                {
                    "picture": picture,
                    "title": title,
                    "date_publish": date_publish,
                    "city": city,
                    "bedroom": bedroom,
                    "description": description,
                    "price": price,
                }
            )


async def tasks_data():
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Mobile Safari/537.36"
    }
    url = f'https://www.kijiji.ca/b-apartments-condos/city-of-toronto/page-100/c37l1700273'
    async with aiohttp.ClientSession() as session:
        response = await session.get(url=url, headers=headers)
        soup = BeautifulSoup(await response.text(), 'lxml')
        pages_count = int(soup.find("div", class_="pagination").find_all("a")[-3].text)
        tasks = []

        for page in range(1, pages_count+1):
            task = asyncio.create_task(get_task_data(session, page))
            tasks.append(task)

        await asyncio.gather(*tasks)


def mainBD():
    engine = create_engine('sqlite:///ddataox.db')
    Apartments.__table__.create(engine)
    session_factory = sessionmaker(bind=engine)
    session: Session
    with session_factory() as session:
        try:
            for _val in house_data:
                row = Apartments(picture=_val["picture"],
                                 title=_val["title"],
                                 date_publish=_val["date_publish"],
                                 city=_val["city"],
                                 bedroom=_val["bedroom"],
                                 description=_val["description"],
                                 price=_val["price"], )
                session.add(row)
            session.commit()
        except SQLAlchemyError as e:
            print(e)
        finally:
            session.close()


def main():

    asyncio.run(tasks_data())
    work_time = datetime.datetime.now().strftime("%d-%m-%Y-%H-%M")
    with open(f"dataox_test_task{work_time}.json", "w") as file:
        json.dump(house_data, file, indent=4, ensure_ascii=False)

    with open(f"dataox_test_task{work_time}.cvs", "w") as file:
        writer = csv.writer(file)

        writer.writerow(
            (
                "picture",
                "title",
                "date_publish",
                "city",
                "bedroom",
                "description",
                "price",
            )
        )

    for house in house_data:
        with open(f"dataox_test_task{work_time}.cvs", "a") as file:
            writer = csv.writer(file)

            writer.writerow(
                (
                    house["picture"],
                    house["title"],
                    house["date_publish"],
                    house["city"],
                    house["bedroom"],
                    house["description"],
                    house["price"],
                )
            )


if __name__ == '__main__':
    main()
    mainBD()
