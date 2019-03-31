SELECT Country, COUNT(*)
FROM Customers
GROUP BY Country
HAVING Country LIKE 'Poland'
    OR Country LIKE 'Germany'
