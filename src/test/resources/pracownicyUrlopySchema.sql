
CREATE TABLE Pracownicy(
  Nr INT PRIMARY KEY,
  Nazwisko NVARCHAR(50) NOT NULL,
  Imie NVARCHAR(50) NOT NULL,
  Stawka INT,
  DataZatrudnienia DATE,
  LiczbaDzieci INT
)

CREATE TABLE Urlopy(
  NrPrac INT,
  OdKiedy DATE,
  DoKiedy DATE,
  PRIMARY KEY(NrPrac,OdKiedy),
  FOREIGN KEY(NrPrac) REFERENCES Pracownicy(Nr)
)
INSERT INTO Pracownicy VALUES
(1, 'Kowal', 'Piotr', 1500, '2010-01-01', 2),
(2, 'Nowak', 'Anna', 1600, '2012-01-01', 1),
(3, 'Wrona', 'Adam', 1100, '2015-01-01', 2)
