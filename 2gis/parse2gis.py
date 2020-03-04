import requests
import os
import psycopg2

def Main():
    all_response = Authorization()
    con = ConnectToDB()
    ExportToDB(con, all_response)
    os.system("pause")


def Authorization():
    try:
        session = requests.Session()
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept - encoding': 'gzip, deflate',
            'accept - language': 'ru-Ru,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'locale': 'ru',
            'origin': 'https://account.2gis.com',
            'pragma': 'no-cache',
            'referer': 'https://account.2gis.com/',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
        }
        json = {
            'login': 'login', #login
            'password': 'password'       #password
        }
        #Авторизация и получение токена
        request_auth = session.post('https://api.account.2gis.com/api/1.0/users/auth', headers=headers, json=json)
        result_requests = request_auth.json()
        token_type = result_requests['result']['token_type']
        access_token = result_requests['result']['access_token']
        token = token_type + ' ' + access_token
        auth = {
            'authorization': token
        }
        #Запрос на получение данных
        get_data = session.get(
            'https://api.account.2gis.com/api/1.0/stat/proxyFeed?method=feed%2Fsession&params=id%3D141275459478985%26size%3D100%26userPlatforms%255B0%255D%3Dstanding%26userPlatforms%255B1%255D%3DiPhone%26userPlatforms%255B2%255D%3DiPad%26userPlatforms%255B3%255D%3Dandroid%26userPlatforms%255B4%255D%3Dmonline%26userPlatforms%255B5%255D%3Dmobile%26userPlatforms%255B6%255D%3DwinPhone%26userPlatforms%255B7%255D%3Dapi',
            headers=auth)
        all_response = get_data.json()['response']
        print('Авторизация прошла успешно')
        print('Количество собранных данных:', len(all_response))
        return all_response
    except:
        print('Не удалось авторизироваться')


def ConnectToDB():
    try:
        con = psycopg2.connect(
            database='database',  #change
            user='user',
            password='password',
            host='host',
            port='port'
        )
        print('Подключение к базе данных прошло успешно')
        return con
    except:
        print('Не удалось подключиться к базе данных')


def ExportToDB(con, all_response):
    try:
        #Создание временной таблицы для помещения собранных данных
        cursor = con.cursor()
        cursor.execute('''CREATE Temp TABLE Test1
        (
        id integer NOT NULL DEFAULT nextval('"2gis_id_seq"'::regclass),
        bctype text COLLATE pg_catalog."default",
        "cardcode" bigint,
        "isnew" smallint,
        lat numeric,
        lon numeric,
        platform text COLLATE pg_catalog."default",
        "sessionid" bigint,
        "sessiontime" timestamp without time zone,
        source text COLLATE pg_catalog."default",
        "sourcetype" text COLLATE pg_catalog."default",
        text text COLLATE pg_catalog."default",
        "time" timestamp without time zone,
        "timeposition" bigint,
        "userclasses" text COLLATE pg_catalog."default",
        "usertime" timestamp without time zone,
        CONSTRAINT Test1_pkey PRIMARY KEY (id));'''
                       )
        print('Временная таблица создана')
        con.commit()
    except:
        print('Не удалось создать временную таблицу')
    try:
        #Экспорт данных во временную таблицу
        for response in all_response:
            if 'text' not in response:
                response['text'] = '-'
            if 'userClasses' not in response:
                response['userClasses'] = '-'
            if 'lat' not in response:
                response['lat'] = '0'
            if 'lon' not in response:
                response['lon'] = '0'
            cursor.execute(
            'INSERT INTO Test1 (bctype, cardcode, isnew,lat, lon, platform, sessionid, sessiontime, source, sourcetype, text, time, timeposition, userclasses, usertime)' +
            'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (response['bcType'], response['cardCode'], response['isNew'], response['lat'], response['lon'],
                response['platform'],
                response['sessionId'], response['sessionTime'], response['source'], response['sourceType'],
                response['text'],
                response['time'], response['timePosition'], str(response['userClasses']), response['userTime']))
            con.commit()
        cursor.execute('SELECT count(*) FROM Test1')
        temp_count = cursor.fetchall()
        print('Во временную базу данных добавлено:', temp_count[0][0])
        cursor.execute('ALTER TABLE Test1 DROP COLUMN id;')
        con.commit()
        cursor.execute(''' UPDATE Test1 
        SET sessiontime = sessiontime + '07:00', time = time + '07:00', usertime = usertime + '07:00';''')
        con.commit()
    except:
        print('Не удалось загрузить данные во временную таблицу')

    try:
        cursor.execute('SELECT count(*) FROM public."2gis"')
        count_before_loading = cursor.fetchall()
        print('Cтрок в таблице 2гис до загрузки:', count_before_loading[0][0])
        #Экспорт данных в основную таблицу с проверкой уже имеющихся в ней данных, исключение занесения
        # в основую таблицу дублирующихся данных
        cursor.execute('''INSERT INTO public."2gis" (bctype, cardcode, isnew,lat, lon, platform,sessionid, sessiontime, source, sourcetype, text, time, timeposition, userclasses, usertime)(
SELECT DISTINCT bctype, cardcode, isnew,lat, lon, platform, sessionid, sessiontime, source, sourcetype, text, time, timeposition, userclasses, usertime FROM Test1
EXCEPT
SELECT bctype, cardcode, isnew,lat, lon, platform, sessionid, sessiontime, source, sourcetype, text, time, timeposition, userclasses, usertime FROM public."2gis");'''
                       )
        con.commit()
        cursor.execute('SELECT count(*) FROM public."2gis"')
        count_after_loading = cursor.fetchall()
        con.commit()
        print('Cтрок в таблице 2гис после загрузки:', count_after_loading[0][0])
        total = count_after_loading[0][0] - count_before_loading[0][0]
        print('В таблицу 2гис вставлено', total, 'строк')
        con.close()
    except:
        print('Не удалось загрузить данные в таблицу 2gis')

Main()