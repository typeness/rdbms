SELECT Orders.OrderID,
       OrderDate,
       Products.ProductID,
       ProductName,
       [Order Details].UnitPrice,
       Quantity,
       CategoryName,
       Customers.CustomerID,
       CompanyName
FROM Customers
       JOIN Orders ON Customers.CustomerID = Orders.CustomerID
       JOIN [Order Details] ON Orders.OrderID = [Order Details].OrderID
       JOIN Products ON [Order Details].ProductID = Products.ProductID
       JOIN Categories ON Products.CategoryID = Categories.CategoryID