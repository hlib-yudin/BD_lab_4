# BD_lab_4
## Комп'ютерний практикум №4 з БД
## Юдін Гліб, КМ-82
#### Варіант 10 -- порівняти найкращий бал з фізики у 2020 та 2019 роках (у кожному регіоні) серед тих, кому було зараховано тест.
-----------------------------------
Даний репозиторій містить наступні файли:
* lab4.py -- python-скрипт, що заповнює колекцію і виконує статистичний запит;
* logs.txt -- файл з вимірами часу на запис даних у БД;
* result_lab4.csv -- результат запиту.

Перед записом даних програма просить ввести URL бази даних MongoDB.  
Також для обробки ситуацій падіння БД створюється додаткова колекція inserted_docs, у яку записується кількість записаних документів для кожного файлу окремо. Щоб змоделювати падіння БД, достатньо під час запиту даних зупинити програму.
