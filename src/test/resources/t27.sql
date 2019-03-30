SELECT P.CategoryID, CategoryName, AVG(UnitPrice)
FROM Products AS P JOIN Categories
ON P.CategoryID=Categories.CategoryID
GROUP BY P.CategoryID, CategoryName