-- Execucao: clickhouse-client --host ... --user ... --password ... < scripts/observability_views.sql
-- Ou cole no console do ClickHouse Cloud (SQL)

CREATE OR REPLACE VIEW observability.v_cost_month_comparison AS
WITH prev AS (
    SELECT toDayOfMonth(cost_date) AS dia, sum(total_usd) AS total_usd, sum(total_brl) AS total_brl
    FROM observability.cost_summary_daily
    WHERE cost_date >= toStartOfMonth(today() - 1) AND cost_date < toStartOfMonth(today())
    GROUP BY dia
),
curr AS (
    SELECT toDayOfMonth(cost_date) AS dia, sum(total_usd) AS total_usd, sum(total_brl) AS total_brl
    FROM observability.cost_summary_daily
    WHERE cost_date >= toStartOfMonth(today()) AND cost_date <= today()
    GROUP BY dia
),
all_dias AS (SELECT number AS dia FROM numbers(1, 31)),
joined AS (
    SELECT a.dia, coalesce(p.total_usd, 0) AS p_usd, coalesce(p.total_brl, 0) AS p_brl,
           coalesce(c.total_usd, 0) AS c_usd, coalesce(c.total_brl, 0) AS c_brl
    FROM all_dias a
    LEFT JOIN prev p ON a.dia = p.dia
    LEFT JOIN curr c ON a.dia = c.dia
)
SELECT dia,
       sum(p_usd) OVER (ORDER BY dia) AS mes_anterior_usd,
       sum(p_brl) OVER (ORDER BY dia) AS mes_anterior_brl,
       sum(c_usd) OVER (ORDER BY dia) AS mes_atual_usd,
       sum(c_brl) OVER (ORDER BY dia) AS mes_atual_brl
FROM joined
ORDER BY dia;
