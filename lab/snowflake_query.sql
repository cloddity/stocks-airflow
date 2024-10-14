SELECT * FROM LAB.ADHOC.MARKET_DATA_FORECAST;

SELECT * FROM LAB.ADHOC.MARKET_DATA_VIEW;

SELECT * FROM LAB.RAW_DATA.MARKET_DATA ORDER BY DATE, SYMBOL;

SELECT SYMBOL, DATE, FORECAST FROM LAB.ANALYTICS.MARKET_DATA WHERE DATE > GETDATE();