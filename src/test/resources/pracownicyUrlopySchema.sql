
CREATE TABLE Pracownicy(
  Nr INT PRIMARY KEY,
  Nazwisko NVARCHAR(50) NOT NULL,
  Imie NVARCHAR(50) NOT NULL,
  Stawka MONEY,
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