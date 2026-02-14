DROP TABLE IF EXISTS VT2511048ACE98__STAGING.transactions;
CREATE TABLE VT2511048ACE98__STAGING.transactions
(
    operation_id           UUID NOT NULL,
	account_number_from    INTEGER NULL,
	account_number_to      INTEGER NULL,
	currency_code          INTEGER NULL,
	country                VARCHAR(30) NULL,
	status                 VARCHAR(30) NULL,
	transaction_type       VARCHAR(30) NULL,
	amount                 INTEGER NULL,
	transaction_dt         TIMESTAMP NULL
)
ORDER BY operation_id
SEGMENTED BY HASH(operation_id) ALL NODES
PARTITION BY transaction_dt::DATE
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);
COMMENT ON TABLE VT2511048ACE98__STAGING.transactions IS 'Движение денежных средств между клиентами в разных валютах';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.operation_id IS 'ID транзакции';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.account_number_from IS 'Внутренний бухгалтерский номер счёта транзакции(ОТ КОГО)';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.account_number_to IS 'Внутренний бухгалтерский номер счёта транзакции(К КОМУ)';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.currency_code IS 'Трёхзначный код валюты страны, из которой идёт транзакция';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.country IS 'Страна-источник транзакции';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.status 
IS 'Статус проведения транзакции
queued: Транзакция в очереди на обработку сервисом
in_progress: Транзакция в обработке
blocked: Транзакция заблокирована сервисом
done: Транзакция выполнена успешно
chargeback: Пользователь осуществил возврат по транзакции';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.transaction_type 
IS 'Тип транзакции во внутреннем учёте
authorisation: Авторизационная транзакция, подтверждающая наличие счёта пользователя
sbp_incoming: Входящий перевод по системе быстрых платежей
sbp_outgoing: Исходящий перевод по системе быстрых платежей
transfer_incoming: Входящий перевод по счёту 
transfer_outgoing: Исходящий перевод по счёту
c2b_partner_incoming Перевод от юридического лица 
c2b_partner_outgoing Перевод юридическому лицу';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.amount IS 'Целочисленная сумма транзакции в минимальной единице валюты страны (копейка, цент, куруш)';
COMMENT ON COLUMN VT2511048ACE98__STAGING.transactions.transaction_dt IS 'Дата и время исполнения транзакции до миллисекунд';


DROP TABLE IF EXISTS VT2511048ACE98__STAGING.currencies;
CREATE TABLE VT2511048ACE98__STAGING.currencies
(
    id                    IDENTITY(1,1), 
	date_update           TIMESTAMP NULL,
	currency_code         INTEGER NULL,
	currency_code_with    INTEGER NULL,
	currency_with_div     NUMERIC(5, 3) NULL
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
COMMENT ON TABLE  VT2511048ACE98__STAGING.currencies IS 'Cправочник, который содержит в себе информацию об обновлениях курсов валют и взаимоотношениях валютных пар друг с другом';
COMMENT ON COLUMN VT2511048ACE98__STAGING.currencies.date_update IS 'Дата обновления курса валют';
COMMENT ON COLUMN VT2511048ACE98__STAGING.currencies.currency_code IS 'Трёхзначный код валюты транзакции';
COMMENT ON COLUMN VT2511048ACE98__STAGING.currencies.currency_code_with IS 'Отношение другой валюты к валюте трёхзначного кода';
COMMENT ON COLUMN VT2511048ACE98__STAGING.currencies.currency_with_div IS 'Значение отношения единицы одной валюты к единице валюты транзакции';
