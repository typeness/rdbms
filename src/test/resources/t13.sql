CREATE TABLE Dbo (
  ColumnA INT CONSTRAINT ColumnAUnique UNIQUE,
  CONSTRAINT Test PRIMARY KEY (ColumnA),
  CONSTRAINT FKey FOREIGN KEY(ColumnA) REFERENCES TAB(B)
)
