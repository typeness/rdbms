SELECT ProductName, CategoryName
FROM Products
       JOIN Categories ON Products.CategoryID = Categories.CategoryID
WHERE CategoryName LIKE 'C%'