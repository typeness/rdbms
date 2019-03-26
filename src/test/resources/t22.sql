SELECT *
FROM Customers
WHERE NOT Region IS NULL
ORDER BY Country, City