# rdbms
rdbms - Relational database management system written in pure FP Scala.

Currently partially supports the following subset of SQL:
##### Data Query Language
- `SELECT [...] FROM [...] WHERE [...] JOIN [...] GROUP BY [...] HAVING [...] ORDER BY [...]`
- `UNION/INTERSECT/EXCEPT`
- `SUM/COUNT/MAX/MIN/AVG`
##### Data Definition Language
- `CREATE TABLE [...]`
- `ALTER TABLE [...]`
- `DROP TABLE [...]`
##### Data Manipulation Language
- `INSERT [...]`
- `UPDATE [...]`
- `DELETE [...]`

#### Examples on Northwind database:

```
SELECT COUNT(*) FROM Customers

Count(*)
--------
91      
```

```
SELECT P.CategoryID, CategoryName, AVG(UnitPrice)
    FROM Products AS P JOIN Categories
    ON P.CategoryID=Categories.CategoryID
    GROUP BY P.CategoryID, CategoryName

P.CategoryID CategoryName   Avg(UnitPrice)    
------------ -------------- ------------------
7            Produce        32.37             
1            Beverages      37.979166666666664
5            Grains/Cereals 20.25             
8            Seafood        20.6825           
6            Meat/Poultry   54.00666666666667 
4            Dairy Products 28.73             
3            Confections    25.16             
2            Condiments     23.0625 
```

```
SELECT * FROM Customers WHERE Country LIKE 'Poland' OR Country LIKE 'Germany'

CustomerID CompanyName              ContactName             ContactTitle         Address            City           Region PostalCode Country Phone         Fax          
---------- ------------------------ ----------------------- -------------------- ------------------ -------------- ------ ---------- ------- ------------- -------------
OTTIK      Ottilies Kaseladen       Henriette Pfalzheim     Owner                Mehrheimerstr. 369 Koln           NULL   50739      Germany 0221-0644327  0221-0765721 
ALFKI      Alfreds Futterkiste      Maria Anders            Sales Representative Obere Str. 57      Berlin         NULL   12209      Germany 030-0074321   030-0076545  
TOMSP      Toms Spezialitaten       Karin Josephs           Marketing Manager    Luisenstr. 48      Munster        NULL   44087      Germany 0251-031259   0251-035695  
LEHMS      Lehmanns Marktstand      Renate Messner          Sales Representative Magazinweg 7       Frankfurt a.M. NULL   60528      Germany 069-0245984   069-0245874  
BLAUS      Blauer See Delikatessen  Hanna Moos              Sales Representative Forsterstr. 57     Mannheim       NULL   68306      Germany 0621-08460    0621-08924   
KOENE      Koniglich Essen          Philip Cramer           Sales Associate      Maubelstr. 90      Brandenburg    NULL   14776      Germany 0555-09876    NULL         
FRANK      Frankenversand           Peter Franken           Marketing Manager    Berliner Platz 43  Munchen        NULL   80805      Germany 089-0877310   089-0877451  
WOLZA      Wolski  Zajazd           Zbyszek Piestrzeniewicz Owner                ul. Filtrowa 68    Warszawa       NULL   01-012     Poland  (26) 642-7012 (26) 642-7012
QUICK      QUICK-Stop               Horst Kloss             Accounting Manager   TaucherstraBe 10   Cunewalde      NULL   01307      Germany 0372-035188   NULL         
WANDK      Die Wandernde Kuh        Rita Muller             Sales Representative Adenauerallee 900  Stuttgart      NULL   70563      Germany 0711-020361   0711-035428  
MORGK      Morgenstern Gesundkost   Alexander Feuer         Marketing Assistant  Heerstr. 22        Leipzig        NULL   04179      Germany 0342-023176   NULL         
DRACD      Drachenblut Delikatessen Sven Ottlieb            Order Administrator  Walserweg 21       Aachen         NULL   52066      Germany 0241-039123   0241-059428  
```
