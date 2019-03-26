SELECT CategoryID, AVG(UnitPrice)
FROM Products
GROUP BY CategoryID