DROP TABLE IF EXISTS VT2511048ACE98__DWH.global_metrics;
CREATE TABLE IF NOT EXISTS VT2511048ACE98__DWH.global_metrics 
(
	date_update                       DATE NOT NULL,
	currency_from                     SMALLINT NOT NULL,
    amount_total                      NUMERIC (17,3) NOT NULL,
    cnt_transactions                  INT NOT NULL,
	avg_transactions_per_account      NUMERIC (17,3) NOT NULL,
	cnt_accounts_make_transactions    INT NOT NULL,
	CONSTRAINT pk PRIMARY KEY (date_update, currency_from) ENABLED
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update::DATE
GROUP BY calendar_hierarchy_day(date_update::DATE, 3, 2);
COMMENT ON TABLE VT2511048ACE98__DWH.global_metrics IS 'Витрина global_metrics';
COMMENT ON COLUMN VT2511048ACE98__DWH.global_metrics.date_update 'Дата расчёта';
COMMENT ON COLUMN VT2511048ACE98__DWH.global_metrics.currency_from 'Код валюты транзакции';
COMMENT ON COLUMN VT2511048ACE98__DWH.global_metrics.amount_total 'Общая сумма транзакций по валюте в долларах';
COMMENT ON COLUMN VT2511048ACE98__DWH.global_metrics.cnt_transactions 'Общий объём транзакций по валюте';
COMMENT ON COLUMN VT2511048ACE98__DWH.global_metrics.avg_transactions_per_account 'Средний объём транзакций с аккаунта';
COMMENT ON COLUMN VT2511048ACE98__DWH.global_metrics.cnt_accounts_make_transactions 'Количество уникальных аккаунтов с совершёнными транзакциями по валюте';

