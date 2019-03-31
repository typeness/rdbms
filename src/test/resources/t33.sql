SELECT DISTINCT Country
FROM Customers
       JOIN Orders ON Customers.CustomerID = Orders.CustomerID