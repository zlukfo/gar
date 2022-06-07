# Импорт ГАР в Postgresql
## Что такое ГАР
**Государственный адресный реестр (ГАР)** – это государственный базовый информационный ресурс, содержащий сведения об адресах и о реквизитах документов о присвоении, об изменении, аннулировании адреса.

ГАР формируется Федеральной налоговой службой России и с августа 2021 года является развитием ФИАС. Сначала был **КЛАДР** - Классификатор адресов РФ, затем **ФИАС** - Федеральная информационная адресная система, и теперь **ГАР**

На сайте ФНС есть онлайн сервис для [поиска](https://fias.nalog.ru/Search) адресов в базе ГАР с точностью до дома

Базу ГАР находится в свободном доступе для [скачивания](https://fias.nalog.ru/Updates) в формате XML. Размер архива базы 33Гб.

Разобраться в структуре базы для выгрузке можно по [схеме данных](https://fias.nalog.ru/docs/gar_schemas.zip)

## Зачем импортировать ГАР в Postgresql
Именно в Postgresql конечно не обязательно - тут дело вкуса, какую БД выбрать, но, в общем, причин иметь собственную БД адресов как минимум две
1. ГАР содержит больше информации, чем выдает поисковый сервис ФНС: в ГАР дополнительно содержится информация о номерах квартирах, комнатах, земельных участках и парковочных местах
2. На основе БД можно реализовать собственный инструмент поиска адреса по административному и мунициальному делению
