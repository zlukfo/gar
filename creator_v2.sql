-- наследуемые (абстрактные) таблицы. содержат обшие для других таблиц поля
CREATE TABLE _required_record (
    ID_         bigint PRIMARY KEY,
    UPDATEDATE  date NOT NULL,
    STARTDATE   date NOT NULL,
    ENDDATE     date NOT NULL,
    ISACTIVE    boolean NOT NULL
);

CREATE TABLE _required_directory (
    NAME        varchar(250) NOT NULL,
    SHORTNAME   varchar(50),
    DESC_        varchar(250)
) INHERITS (_required_record);

CREATE TABLE _required_object (
    OBJECTID    bigint NOT NULL, 
    CHANGEID    bigint NOT NULL,
    PREVID      bigint,
    NEXTID      bigint
);

-- таблицы справочников
CREATE TABLE ADDRESSOBJECTTYPE (
    LEVEL int NOT NULL
) INHERITS (_required_directory);
CREATE TABLE APARTMENTTYPE () INHERITS (_required_directory);
CREATE TABLE HOUSETYPE () INHERITS (_required_directory);
CREATE TABLE ROOMTYPE () INHERITS (_required_directory);
CREATE TABLE PARAMTYPE (
    CODE varchar(50) NOT NULL
) INHERITS (_required_directory);

-- таблицы для загрузки исходных данных
-- объекты (деспублика, город,... с точностью до улицы)
CREATE TABLE OBJECT (
    OBJECTGUID  varchar(36) NOT NULL,
    OPERTYPEID  bigint NOT NULL,
    NAME        varchar(250) NOT NULL,
    ISACTUAL    boolean NOT NULL,
    TYPENAME    varchar(50) NOT NULL,
    LEVEL       varchar(10) NOT NULL
) INHERITS (_required_object, _required_record);
-- дома
CREATE TABLE HOUSE (
    OBJECTGUID  varchar(36) NOT NULL,
    ISACTUAL    boolean NOT NULL,
    OPERTYPEID  bigint NOT NULL,
    HOUSENUM    varchar(50),
    ADDNUM1     varchar(50),
    ADDNUM2     varchar(50),
    HOUSETYPE   bigint,
    ADDTYPE1    bigint,
    ADDTYPE2    bigint
) INHERITS (_required_object, _required_record);
-- помещения
CREATE TABLE APARTMENT (
    OBJECTGUID  varchar(36) NOT NULL,
    ISACTUAL    boolean NOT NULL,
    OPERTYPEID  bigint NOT NULL,
    NUMBER varchar(50) NOT NULL,
    APARTTYPE bigint NOT NULL
) INHERITS (_required_object, _required_record);
-- комнаты
CREATE TABLE ROOM (
    OBJECTGUID  varchar(36) NOT NULL,
    ISACTUAL    boolean NOT NULL,
    OPERTYPEID  bigint NOT NULL,
    NUMBER varchar(50) NOT NULL,
    ROOMTYPE bigint NOT NULL
) INHERITS (_required_object, _required_record);

CREATE INDEX objectid_object ON object using hash  (objectid); 
CREATE INDEX objectid_house ON house using hash  (objectid); 
CREATE INDEX objectid_apartment ON apartment using hash  (objectid); 
CREATE INDEX objectid_room ON room using hash  (objectid); 

CREATE TABLE ITEM_ADM (
    ID_         bigint,
    UPDATEDATE  date NOT NULL,
    STARTDATE   date NOT NULL,
    ENDDATE     date NOT NULL,
    ISACTIVE    boolean NOT NULL,
    PARENTOBJID bigint,
    OBJECTID    bigint NOT NULL, 
    CHANGEID    bigint NOT NULL,
    PREVID      bigint,
    NEXTID      bigint,
    REGIONCODE varchar(4),
    AREACODE varchar(4),
    CITYCODE varchar(4),
    PLACECODE varchar(4),
    PLANCODE varchar(4),
    STREETCODE varchar(4),
    PATH bigint[] NOT NULL
)  PARTITION BY LIST (REGIONCODE);

-- функция создает секции таблицы
-- первый параметр - имя сущесвующей секционируемой таблицы
-- второй сколько секций создать 
-- признак секционирования - номер счетчика для секции 
CREATE OR REPLACE FUNCTION create_section_tables(text, int) RETURNS text AS $$
DECLARE
  n int := 1;
BEGIN
  LOOP
   IF n>$2 THEN
   	EXIT;
   END IF;
	EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES IN (%s);', $1 || '_' || n, $1, n);
    n:= n+1;
  END LOOP;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

SELECT create_section_tables('item_adm', 100);
CREATE INDEX ON ITEM_ADM (REGIONCODE);

-- поисковая таблица
CREATE TABLE SEARCH (
    FULLADDRESS  varchar(1000) NOT NULL,
    FTS   tsvector NOT NULL,
    FOR_NOMINATIM varchar(500) NOT NULL,
    STRUCT  jsonb,
    REGIONCODE varchar(4)
) PARTITION BY LIST (REGIONCODE);

CREATE INDEX ON SEARCH (REGIONCODE);

SELECT create_section_tables('search', 100);

CREATE INDEX search_fts ON SEARCH USING GIN (FTS);

-- конфигурация fts для tsvector
CREATE TEXT SEARCH CONFIGURATION address (
    PARSER = 'default'
);
ALTER TEXT SEARCH CONFIGURATION address
    ADD MAPPING FOR uint, word, hword, hword_part, int WITH russian_stem;

-- функция восстановления полного адреса из таблицы item выбирает только актуральные и активные записи 
-- пример использования select get_full_address("path") from item limit 10
CREATE FUNCTION get_full_address(bigint[]) RETURNS text AS $$
DECLARE
  s text[] := ARRAY[]::text[];
  x bigint;
  n text;
BEGIN
  FOREACH x IN ARRAY $1
  LOOP
    select concat_ws(' ', lower(addressobjecttype.name), object.name) into n
    from object inner join addressobjecttype  
    on addressobjecttype.level = object.level::integer and addressobjecttype.shortname = object.typename
    where "objectid"=x and object.isactive=true and object.isactual=true;
    s:=s || n;
    CONTINUE WHEN n IS NOT NULL;

    select concat_ws(' ', lower(housetype.name), house.housenum) into n
    from house inner join housetype  
    on housetype.id_ = house.housetype::integer 
    where "objectid"=x and house.isactive=true and house.isactual=true;
    s:=s || n;
    CONTINUE WHEN n IS NOT NULL;

    select concat_ws(' ', lower(apartmenttype.name), apartment.number) into n
    from apartment inner join apartmenttype  
    on apartmenttype.id_ = apartment.aparttype::integer 
    where "objectid"=x and apartment.isactive=true and apartment.isactual=true;
    s:=s || n;
    CONTINUE WHEN n IS NOT NULL;

    select concat_ws(' ', lower(roomtype.name), room.number) into n
    from room inner join roomtype  
    on roomtype.id_ = room.roomtype::integer 
    where "objectid"=x and room.isactive=true and room.isactual=true;
    s:=s || n;
  END LOOP;
  RETURN array_to_string(s, ', ');
END;
$$ LANGUAGE plpgsql;

-- функция получения tsvector
CREATE OR REPLACE FUNCTION get_fts_address(bigint[]) RETURNS tsvector AS $$
DECLARE
  s text[] := ARRAY[]::text[];
  x bigint;
  n text;
BEGIN
  FOREACH x IN ARRAY $1
  LOOP
    select name into n from object where "objectid"=x and isactive=true and isactual=true;
    s:=s || n;
    CONTINUE WHEN n IS NOT NULL;
    select housenum into n from house where "objectid"=x and isactive=true and isactual=true;
    s:=s || n;
    CONTINUE WHEN n IS NOT NULL;
    select number into n from apartment where "objectid"=x and isactive=true and isactual=true;
    s:=s || n;
    CONTINUE WHEN n IS NOT NULL;
    select number into n from room where "objectid"=x and isactive=true and isactual=true;
    s:=s || n;
  END LOOP;
  RETURN to_tsvector('address', array_to_string(s, ' '));
END;
$$ LANGUAGE plpgsql;

-- функция получения для nominatim
CREATE OR REPLACE FUNCTION get_for_nominatim(bigint[]) RETURNS text AS $$
DECLARE
  s text[] := ARRAY[]::text[];
  x bigint;
  n text;
BEGIN
  FOREACH x IN ARRAY $1
  LOOP
    select name into n from object where "objectid"=x and isactive=true and isactual=true;
    s:=s || n;
    CONTINUE WHEN n IS NOT NULL;
    select housenum into n from house where "objectid"=x and isactive=true and isactual=true;
    s:=s || n;
  END LOOP;
  RETURN array_to_string(s, ' ');
END;
$$ LANGUAGE plpgsql;

-- триггер - при инморте (довалении) записи в таблицу import автоматически добавляется запись в таблицу search
CREATE OR REPLACE FUNCTION add_to_search() RETURNS trigger AS $add_to_search$
    BEGIN
        IF NEW.path IS NOT NULL THEN
			INSERT INTO search (fulladdress, fts, for_nominatim, struct, regioncode)
    			VALUES (get_full_address(NEW.path), get_fts_address(NEW.path), get_for_nominatim(NEW.path), json_build_object('isactive', NEW.isactive, 'updatedate', NEW.updatedate, 'objectid', NEW.objectid, 'id', NEW.id_), NEW.regioncode);
		END IF;
        RETURN NEW;
    END;
$add_to_search$ LANGUAGE plpgsql;

CREATE TRIGGER add_to_search AFTER INSERT ON item_adm
    FOR EACH ROW EXECUTE FUNCTION add_to_search();

CREATE TABLE GAR_SEARCH2 (
    FULLADDRESS  varchar(1000) NOT NULL,
    FTS   tsvector NOT NULL,
    FOR_NOMINATIM varchar(500) NOT NULL,
    STRUCT  jsonb,
    REGIONCODE varchar(4)
);

-- CREATE INDEX rumidx2 ON gar_search2 USING rum (fts rum_tsvector_ops);

CREATE OR REPLACE FUNCTION add_to_search2() RETURNS trigger AS $add_to_search2$
    BEGIN
        IF NEW.path IS NOT NULL THEN
			INSERT INTO gar_search2 (fulladdress, fts, for_nominatim, struct, regioncode)
    			VALUES (get_full_address(NEW.path), get_fts_address(NEW.path), get_for_nominatim(NEW.path), json_build_object('isactive', NEW.isactive, 'updatedate', NEW.updatedate, 'objectid', NEW.objectid, 'id', NEW.id_), NEW.regioncode);
		END IF;
        RETURN NEW;
    END;
$add_to_search2$ LANGUAGE plpgsql;

CREATE TRIGGER add_to_search2 AFTER INSERT ON item_adm
    FOR EACH ROW EXECUTE FUNCTION add_to_search2();
