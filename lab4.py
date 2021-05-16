"""Юдін Гліб, КМ-82, лабораторна робота №1
Варіант 10
Порівняти найкращий бал з фізики у 2020 та 2019 роках (у кожному регіоні) серед тих,
кому було зараховано тест.
"""

from pymongo import MongoClient
import csv
import datetime

url = input("Введіть MongoDB URL (default -- mongodb://localhost:27017): ")
url = url if url else "mongodb://localhost:27017"
client = MongoClient(url)

# під'єднуємось до бази даних
db = client.db_zno

with open('logs.txt', 'w') as logs_file:
    batch_size = 1000 # розмір однієї групи документів
    file_names = ["Odata2019File.csv", "Odata2020File.csv"]
    years = [2019, 2020]

    for j in range(2):
        file_name, year = file_names[j], years[j]
        with open(file_name, "r", encoding="cp1251") as csv_file:

            # записуємо час початку запису файлів
            start_time = datetime.datetime.now() 
            print(f"Читаємо файл {file_name}...")
            logs_file.write(str(start_time) + ", " + file_name + ' -- початок запису\n')
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            i = 0 # кількість вставлених документів з поточної групи
            batches_num = 0 # кількість вставлених груп
            document_bundle = []

            # знаходимо, скільки документів вже вставлено в колекцію
            num_inserted = db.inserted_docs.find_one({"year": year})
            if num_inserted == None:
                num_inserted = 0
            else:
                num_inserted = num_inserted["num_docs"]
                print(f"Пропускаємо {num_inserted} документів...")

            # читаємо csv-файл; кожен його рядок -- це словник
            for row in csv_reader:

                # якщо n документів вже записано, то перші n документів пропускаємо
                if batches_num * batch_size + i < num_inserted:
                    i += 1
                    if i == batch_size:
                        i = 0
                        batches_num += 1
                    continue

                document = row
                document['year'] = year
                document_bundle.append(document)
                i += 1
                # якщо назбиралось 1000 документів -- записуємо дані
                if i == batch_size:
                    i = 0
                    batches_num += 1
                    db.collection_zno_data.insert_many(document_bundle)
                    document_bundle = []
                    # також записуємо в окремий документ кількість вставлених рядків
                    if batches_num == 1:
                        db.inserted_docs.insert_one({"num_docs": batch_size, "year": year})
                    else:
                        db.inserted_docs.update_one({
                            "year": year, "num_docs": (batches_num - 1) * batch_size}, 
                            {"$inc": {
                                "num_docs": batch_size
                            }  })
            # якщо файл скінчився, а група документів не повна -- записуємо її
            if i != 0 and document_bundle:
                db.inserted_docs.update_one({
                    "year": year, "num_docs": batches_num * batch_size}, 
                    {"$inc": {
                        "num_docs": i
                    }  })
                db.collection_zno_data.insert_many(document_bundle)

            end_time = datetime.datetime.now() 
            logs_file.write(str(end_time) + ", " + file_name + ' -- кінець запису\n')
            print('time:', end_time - start_time)


# сам статистичний запит
# спочатку фільтруємо дані (physTestStatus -- зараховано)
# потім -- групуємо за областю і роком, знаходимо максимальний бал
# записуємо результати в окремий файл
result = db.collection_zno_data.aggregate([

    {"$match": {"physTestStatus": "Зараховано"}},

    {"$group": {
        "_id": {
            "year": "$year",
            "regname": "$REGNAME"
        },
        "max_score": {
            "$max": "$physBall100" 
        } 
    }},

    {"$sort": {"_id": 1} }
])

with open('result_lab4.csv', 'w', encoding="utf-8") as new_csv_file:
    csv_writer = csv.writer(new_csv_file)
    csv_writer.writerow(['Область', 'Рік', 'Макс. бал з фізики'])
    for document in result:
        year = document["_id"]["year"]
        regname = document["_id"]["regname"]
        max_score = document["max_score"]
        csv_writer.writerow([regname, year, max_score])
