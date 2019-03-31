SELECT ProductName
FROM Products
       JOIN Categories ON Products.CategoryID = Categories.CategoryID
WHERE CategoryName LIKE 'Beverages'
  AND UnitPrice BETWEEN 20 AND 30