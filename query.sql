    select
        fs.instrument_id,
        fs.when_timestamp,
        fs.gamma as gamma_5s,
        om.gamma as gamma_1m,
        tm.gamma as gamma_30m,
        oh.gamma as gamma_60m,
        fs.vega as vega_5s,
        om.vega as vega_1m,
        tm.vega as vega_30m,
        oh.vega as vega_60m,
        fs.theta as theta_5s,
        om.theta as theta_1m,
        tm.theta as theta_30m,
        oh.theta as theta_60m
    from
        fs inner join om on fs.instrument_id=om.instrument_id
        inner join tm on fs.instrument_id=tm.instrument_id
        inner join oh on fs.instrument_id=oh.instrument_id;