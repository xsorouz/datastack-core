-- dbt/models/cleaned_sales.sql
-- Exemple générique de transformation : nettoyage d’une table ventes

SELECT
    id,
    customer_id,
    amount,
    date,
    status
FROM {{ source('public', 'sales') }}
WHERE amount IS NOT NULL
  AND status != 'cancelled';
