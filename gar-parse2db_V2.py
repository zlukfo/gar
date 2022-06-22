import psycopg2
from lxml import etree
import os
import re
#COUNT=-1                         # количество файлов для парсинга (-1 все файлы каталога) !!! НЕ НУЖНО, убрать !!!
EXCUDE_TAGS=[]                  # тэги, которые не нужно парсить (getTreeByXml)

# ПОДГОТОВКА СПИСКА ФАЙЛОВ ДЛЯ ПАРСИНГА
PATH4PARSE=[
    #'C:\\tmp\\fias\\spravochnik', 
    'C:\\tmp\\fias\\rostov', 
    #'C:\\tmp\\fias\\adigeya'
    ]
DATA4PARSE=[]
for path in PATH4PARSE:
    d={}
    for address, dirs, files in os.walk(path):
        for name in files:
            filepath=os.path.join(address, name)
            '''
            #--------загрузка справочников (единожны)
            if re.search('AS_ADDR_OBJ_TYPES_\d',filepath):
                d['ADDRESSOBJECTTYPE']=filepath
            if re.search('AS_APARTMENT_TYPES_\d',filepath):
                d['APARTMENTTYPE']=filepath
            if re.search('AS_HOUSE_TYPES_\d',filepath):
                d['HOUSETYPE']=filepath
            if re.search('AS_ROOM_TYPES_\d',filepath):
                d['ROOMTYPE']=filepath

            #if re.search('AS_PARAM_TYPES_\d',filepath):
            #    d['PARAMTYPE']=filepath
            #---------- загрузка объектов
            if re.search('AS_HOUSES_\d',filepath):
                d['HOUSE']=filepath
            if re.search('AS_ADDR_OBJ_\d',filepath):
                d['OBJECT']=filepath
            if re.search('AS_APARTMENTS_\d',filepath):
                d['APARTMENT']=filepath
            if re.search('AS_ROOMS_\d',filepath):
                d['ROOM']=filepath
            '''
            #--------- загрузка иерархий
            # !!! это запускать вторым этапом когда все объекты и справочники загружены
            
            if 'AS_ADM_HIERARCHY' in filepath:
                d['ITEM_ADM']=filepath
            
    DATA4PARSE.append(d)

'''
DATA4PARSE = {
    #'OBJECT': 'C:\\tmp\\fias\\adigeya\\++AS_ADDR_OBJ_20220505_ac6ec8d7-bf35-48f6-9885-7e1c5ccd40ca.XML'
    #'HOUSE': 'C:\\tmp\\fias\\adigeya\\++AS_HOUSES_20220505_3dd848fb-0be3-4466-9bb3-f983b8f3e0d7.XML'
    'ITEM_ADM': 'C:\\tmp\\fias\\adigeya\\++AS_ADM_HIERARCHY_20220505_5ec4caaf-a99e-430b-9e0a-d75312416288.XML',
    #'APARTMENT': 'C:\\tmp\\fias\\adigeya\\++AS_APARTMENTS_20220505_c43659ea-737e-4fd0-b79c-09f528ade61b.XML',
    #'ROOM': 'C:\\tmp\\fias\\adigeya\\++AS_ROOMS_20220505_2e61d73a-af6b-4081-947f-440c177b8c13.XML'

    #'ADDRESSOBJECTTYPE': 'C:\\tmp\\fias\\spravochnik\\AS_ADDR_OBJ_TYPES_20220505_0bb6fa75-189d-48b1-b141-1e015c42bc33.XML',
    # --'OBJECTLEVEL': 'C:\\tmp\\fias\\spravochnik\\AS_OBJECT_LEVELS_20220505_9f4caad2-eccf-4c85-9086-d7cda006c1cc.XML',
    #'APARTMENTTYPE': 'C:\\tmp\\fias\\spravochnik\\AS_APARTMENT_TYPES_20220505_688eb2fb-d430-4639-b8a5-78f262e2690b.XML',
    #'HOUSETYPE': 'C:\\tmp\\fias\\spravochnik\\AS_HOUSE_TYPES_20220505_d0a61098-045c-4486-aa56-038635096306.XML',
    #'ROOMTYPE': 'C:\\tmp\\fias\\spravochnik\\AS_ROOM_TYPES_20220505_0a4a416d-675a-41fb-9e6d-f0b0936f3960.XML'
    #--- это скорее всего не надо 'ITEM': 'C:\\tmp\\fias\\adigeya\\++AS_MUN_HIERARCHY_20220505_6f177595-dfe5-4a80-b07c-54e739fc53f3.XML',
}
'''


conn = psycopg2.connect(host='localhost',
                        port='5555',
                        user='postgres',
                        password='123qweasdZ',
                        database='sfera')
cursor = conn.cursor()


def getTreeByXml(file_object, RECORD_TAG):
    record={}
    context = etree.iterparse(file_object, events=("start","end"))
    for event, elem in context:
        attrib=dict(elem.attrib)
        tag=etree.QName(elem.tag).localname
        if not attrib or tag in EXCUDE_TAGS:
            continue
        if event=="end" and tag==RECORD_TAG:
            yield record
            elem.clear()
            record={}
            continue
        record.setdefault(tag, attrib)
        if record[tag] != attrib:
            try:
                record[tag].append(attrib)
            except:
                record[tag]=[record[tag], attrib]


for region in DATA4PARSE:
    for table_name, filename in region.items():
        print ('\r\n',filename)
        fr=open(filename,'rb')
        record_tag = table_name
        if table_name == 'ITEM_ADM':
            record_tag = 'ITEM'
        
        COUNT=-1
        
        for rec in getTreeByXml(fr, record_tag):
            if not COUNT:
                break
            rec=rec[record_tag]
            print(f'\rОбработано: ({COUNT})', end='')

            #----------подготовка данных для вставки в sql-запрос
            if 'PATH' in rec:
                rec['PATH']= '{'+rec['PATH'].replace('.', ',')+'}'
            fields_name = '"'+'", "'.join(rec.keys())+'"'
            fields_name=fields_name.lower().replace('"id"', '"id_"').replace('"desc"', '"desc_"')
            fields_values = "'"+"', '".join(rec.values())+"'"

            cursor.execute(f'INSERT INTO gar_{table_name} ({fields_name}) VALUES({fields_values})') 
            COUNT-=1
        conn.commit() 
cursor.close()
conn.close()
