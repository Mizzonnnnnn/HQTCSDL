-- LAB 1
-- Bài 1: Truy vấn danh sách các 

SET STATISTICS TIME ON;
SELECT * from Customer
SET STATISTICS TIME OFF;


/* Bài 2
Truy vấn danh sách các Customer theo các thông tin Id, FullName (là kết hợp FirstName-
LastName), City, Country
*/

SET STATISTICS TIME ON;
SELECT Id, CONCAT(FirstName, ' ', LastName) AS FullName, 
       City, Country
FROM Customer;
SET STATISTICS TIME OFF;

/* Bài 3
Cho biết có bao nhiêu khách hàng từ Germany và UK, đó là những khách hàng nào 
*/

SET STATISTICS TIME ON;
SELECT Country, COUNT(*) AS CustomerCount,
       STRING_AGG(CONCAT(FirstName, ' ', LastName), ', ') AS CustomerList
FROM Customer 
WHERE Country IN ('Germany', 'UK')
GROUP BY Country
SET STATISTICS TIME OFF;

/* Bài 4:
Liệt kê danh sách khách hàng theo thứ tự tăng dần của FirstName và giảm dần của Country
*/

SET STATISTICS TIME ON;
SELECT Id, FirstName, LastName, City, Country 
FROM Customer
ORDER BY FirstName, LastName DESC
SET STATISTICS TIME OFF;

/* Bài 5
Truy vấn danh sách các khách hàng với ID là 5,10, từ 1-10, và từ 5-10
*/
SET STATISTICS TIME ON;
SELECT Id, FirstName, LastName, City, Country 
FROM Customer
WHERE Id IN (5,10)
SELECT Id, FirstName, LastName, City, Country 
FROM Customer
WHERE Id BETWEEN 5 AND 10
SET STATISTICS TIME ON;
SELECT Id, FirstName, LastName, City, Country 
FROM Customer
WHERE Id BETWEEN 1 AND 10
SET STATISTICS TIME OFF;

/* Bài 6
Truy vấn các khách hàng ở các sản phẩm (Product) mà đóng gói dưới dạng bottles có 
giá từ 15 đến 20 mà không từ nhà cung cấp có ID là 16. 
*/
SET STATISTICS TIME ON;
SELECT  c.Id AS CustomerId, CONCAT(c.FirstName, ' ', c.LastName) AS FullName, 
		c.City, c.Country, c.Phone, p.UnitPrice, p.Package
FROM Customer c
    JOIN [Order] o ON c.Id = o.CustomerId
    JOIN OrderItem ot ON o.Id = ot.OrderId
    JOIN Product p ON ot.ProductId = p.Id
WHERE p.Package LIKE '%bottles%' 
    AND p.UnitPrice BETWEEN 15 AND 20 
    AND p.SupplierId <> 16;
SET STATISTICS TIME OFF;

-- LAB2 
/* Bài 1
Xuất danh sách các nhà cung cấp (gồm Id, CompanyName, ContactName, City, Country, 
Phone) kèm theo giá min và max của các sản phẩm mà nhà cung cấp đó cung cấp. Có 
sắp xếp theo thứ tự Id của nhà cung cấp (Gợi ý : Join hai bản Supplier và Product, 
dùng GROUP BY tính Min, Max)
*/
SET STATISTICS TIME ON;
Select s.Id, s.CompanyName, s.ContactName, s.City, s.Country, s.Phone, 
	MIN(p.UnitPrice) as MinProductPrice, MAX(p.UnitPrice) as MaxProductPrice
From Supplier s
JOIN Product p ON s.Id = p.SupplierId
Group by s.Id, s.CompanyName, s.ContactName, s.City, s.Country, s.Phone
SET STATISTICS TIME OFF;

/* Bài 2
Cũng câu trên nhưng chỉ xuất danh sách nhà cung cấp có sự khác biệt giá (max – min) 
không quá lớn (<=30).(Gợi ý: Dùng HAVING)
*/
SET STATISTICS TIME ON;
Select s.Id, s.CompanyName, s.ContactName, s.City, s.Country, s.Phone, 
	(MAX(p.UnitPrice)-MIN(p.UnitPrice)) as PriceDifference
From Supplier s
JOIN Product p ON s.Id = p.SupplierId
Group by s.Id, s.CompanyName, s.ContactName, s.City, s.Country, s.Phone
Having (MAX(p.UnitPrice)-MIN(p.UnitPrice)) <= 30
SET STATISTICS TIME OFF;

/* Bài 3
Xuất danh sách các hóa đơn (Id, OrderNumber, OrderDate) kèm theo tổng giá chi trả 
(UnitPrice*Quantity) cho hóa đơn đó, bên cạnh đó có cột Description là “VIP” nếu 
tổng giá lớn hơn 1500 và “Normal” nếu tổng giá nhỏ hơn 1500(Gợi ý: Dùng UNION)
*/

SET STATISTICS TIME ON;
Select o.Id, o.OrderNumber, o.OrderDate, 
	   SUM(ot.UnitPrice*ot.Quantity) as TotalPayment, 
	   'VIP' as Description
From "Order" o
JOIN OrderItem ot ON o.Id = ot.OrderId
Group by o.Id, o.OrderNumber, o.OrderDate
Having SUM(ot.UnitPrice*ot.Quantity) > 1500
union
Select o.Id, o.OrderNumber, o.OrderDate, 
       SUM(ot.UnitPrice*ot.Quantity) as TotalPayment, 
	   'NORMAL' as Description
From "Order" o
JOIN OrderItem ot ON o.Id = ot.OrderId
Group by o.Id, o.OrderNumber, o.OrderDate
Having  SUM(ot.UnitPrice*ot.Quantity) < 1500
SET STATISTICS TIME OFF;

/* Bài 4
Xuất danh sách những hóa đơn (Id, OrderNumber, OrderDate) trong tháng 7 nhưng 
phải ngoại trừ ra những hóa đơn từ khách hàng France. (Gợi ý: dùng EXCEPT)
*/

SET STATISTICS TIME ON;
Select o.Id, o.OrderNumber, o.OrderDate
From "Order" o
Where Month(OrderDate) = 7
except
Select o.Id, o.OrderNumber, o.OrderDate
From "Order" o
JOIN Customer c ON o.CustomerId = c.Id
Where c.Country = 'France'
SET STATISTICS TIME OFF;

/* Bài 5
Xuất danh sách những hóa đơn (Id, OrderNumber, OrderDate, TotalAmount) nào 
có TotalAmount nằm trong top 5 các hóa đơn. (Gợi ý : Dùng IN)
*/

SET STATISTICS TIME ON;
Select Id, OrderNumber, OrderDate, TotalAmount
From "Order"
Where TotalAmount IN (
	Select TOP 5 TotalAmount
	From  "Order"
	Order BY  TotalAmount DESC
)
Order by TotalAmount DESC
SET STATISTICS TIME OFF;

--LAB 3
/* Bài 1
Sắp xếp sản phẩm tăng dần theo UnitPrice, và tìm 20% dòng có UnitPrice cao nhất (Lưu ý: 
Dùng ROW_NUMBER )
*/
SET STATISTICS TIME ON;
SELECT *
FROM 
(
	SELECT RowNum, Id, ProductName, UnitPrice, MAX(RowNum) OVER () as RowLast
	From (
		Select ROW_NUMBER() Over (Order by UnitPrice DESC) as RowNum, 
		Id, ProductName, UnitPrice
		From Product
	) As DerivedTable
) Report
Where Report.RowNum <= CEiling(0.2 * RowLast)
ORDER BY UnitPrice ASC;
SET STATISTICS TIME OFF;

/* Bài 2
Với mỗi hóa đơn, xuất danh sách các sản phẩm, số lượng (Quantity) và số phần trăm của sản 
phẩm đó trong hóa đơn. (Gợi ý: ta lấy Quantity chia cho tổng Quantity theo hóa đơn * 100 
+ ‘%’. Dùng SUM … OVER)
*/
SET STATISTICS TIME ON;
select p.Id, p.ProductName, p.UnitPrice, ot.Quantity,
	STR(ot.Quantity * 100.0 / sum(ot.Quantity) over (partition by o.Id), 5,2) + '%' as percentage
From OrderItem ot
	JOIN Product p ON ot.ProductId = p.Id
	JOIN "Order" o ON ot.OrderId = o.Id
Order by p.Id
SET STATISTICS TIME OFF;

/* Bài 3
Xuất danh sách các nhà cung cấp kèm theo các cột USA, UK, France, Germany, Others. Nếu nhà 
cung cấp nào thuộc các quốc gia  này thì ta đánh số 1 còn lại là 0 (Gợi ý: Tạo bảng tạm theo 
chiều dọc trước với tên nhà cung cấp và thuộc quốc gia USA, UK, France, Germany hay Others. 
Sau đó PIVOT bảng tạm này để tạo kết quả theo chiều ngang)
*/
SET STATISTICS TIME ON;
select 
	SupplierPivot.Id as SupplierID,
	SupplierPivot.CompanyName, 
	ISNULL(SupplierPivot.[USA],0) AS USA,
	ISNULL(SupplierPivot.[UK], 0) AS UK,
	ISNULL(SupplierPivot.[France], 0) AS France,
	ISNULL(SupplierPivot.[Germany], 0) AS Germany,
	ISNULL(SupplierPivot.[Others], 0) AS Others
From (
	Select
	    Id, 
		CompanyName,
        CASE 
            WHEN Country = 'USA' THEN 'USA'
            WHEN Country = 'UK' THEN 'UK'
            WHEN Country = 'France' THEN 'France'
            WHEN Country = 'Germany' THEN 'Germany'
            ELSE 'Others'
        END AS CountryGroup,
        1 AS Flag
    FROM Supplier
) as SupplierTemp
Pivot (
	Max(Flag) for CountryGroup in ([USA], [UK], [France], [Germany], [Others])
) as SupplierPivot
SET STATISTICS TIME OFF;

/* Bài 4
Xuất danh sách các hóa đơn gồm OrderNumber, OrderDate (format: dd mm yyyy), CustomerName, 
Address (format: “Phone: …… , City: …. and Country: ….”), TotalAmount làm tròn không chữ 
số thập phân và đơn vị theo kèm là Euro) 
*/

SET STATISTICS TIME ON;
SELECT o.OrderNumber,
    OrderDate = CONVERT(VARCHAR(10), o.OrderDate, 103),
    CustomerName = 'Customer' + SPACE(1) + ':' + c.FirstName + SPACE(1) + c.LastName,
    Address = 'Phone' + SPACE(1) + ':' + c.Phone + ',' + SPACE(1) + 
              'City' + SPACE(1) + ':' + c.City + SPACE(1) + 
              'and' + SPACE(1) + 'Country' + SPACE(1) + ':' + c.Country,
    Amount = LTRIM(STR(CAST(ROUND(o.TotalAmount, 0) AS INT))) + SPACE(1) + 'Euro'
FROM "Order" o
INNER JOIN Customer c ON o.CustomerId = c.Id
ORDER BY o.OrderDate DESC;
SET STATISTICS TIME OFF;

/* Bài 5
Xuất danh sách các sản phẩm dưới dạng đóng gói bags. Thay đổi chữ bags thành ‘túi’ 
(Lưu ý: để dùng tiếng việt có dấu ta ghi chuỗi dưới dạng N’túi’)
*/

SET STATISTICS TIME ON;
select Id, ProductName, SupplierId, UnitPrice, 
		Package = Stuff(Package, CHARINDEX('bags',Package), Len('bag'), 'túi')
From Product
where Package Like '%bags%'
SET STATISTICS TIME OFF;

/* Bài 6
Xuất danh sách các khách hàng theo tổng số hóa đơn mà khách hàng đó có, sắp xếp theo 
thứ tự giảm dần của tổng số hóa đơn, kèm theo đó là  các thông tin phân hạng DENSE_RANK 
và nhóm (chia thành 3 nhóm) (Gợi ý: dùng NTILE(3) để chia nhóm. 
*/

SET STATISTICS TIME ON;
SELECT 
    CustomerID = Report.Id,
    CustomerName = Report.FirstName + ' ' + Report.LastName,
    TotalOrders = Report.OrderCount,
    CustomerRank = DENSE_RANK() OVER (ORDER BY Report.OrderCount DESC),
    [Group] = NTILE(3) OVER (ORDER BY Report.OrderCount DESC)
FROM
(
    SELECT C.Id, C.FirstName, C.LastName, COUNT(O.Id) AS OrderCount
    FROM Customer C
    LEFT JOIN [Order] O ON C.Id = O.CustomerId
    GROUP BY C.Id, C.FirstName, C.LastName
) Report
ORDER BY Report.OrderCount DESC;
SET STATISTICS TIME OFF;

-- LAB 4
/* Bài 1
Theo mỗi  OrderID cho biết số lượng Quantity của mỗi ProductID chiếm tỷ lệ bao nhiêu phần trăm
*/

SET STATISTICS TIME ON;
SELECT 
    OrderId, 
    ProductId, 
    Quantity,
    SUM(Quantity) OVER (PARTITION BY ProductId) AS TotalQuantityByProduct,
    STR(Quantity * 100.0 / SUM(Quantity) OVER (PARTITION BY ProductId), 5, 2) + '%' AS PercentByProduct
FROM OrderItem;
SET STATISTICS TIME OFF;

/* Bài 2
Xuất các hóa đơn kèm theo thông tin ngày trong tuần của hóa đơn là : Thứ 2, 3,4,5,6,7, Chủ Nhậ
*/

SET STATISTICS TIME ON;
SELECT Id AS OrderId, OrderNumber, OrderDate, 
	   DATENAME(WEEKDAY, OrderDate) AS WeekDayName
FROM [Order] ;
SET STATISTICS TIME OFF;

/* Bài 3
Với mỗi ProductID trong OrderItem xuất các thông tin gồm OrderID, ProductID, ProductName, 
UnitPrice, Quantity, ContactInfo, ContactType. Trong đó ContactInfo ưu tiên Fax, nếu không 
thì dùng Phone của Supplier sản phẩm đó. Còn ContactType là ghi chú đó là loại ContactInfo nào
*/

SET STATISTICS TIME ON;
SELECT oi.OrderID, oi.ProductID, 
	   p.ProductName, p.UnitPrice, oi.Quantity, 
	   ISNULL(s.Fax, s.Phone) AS ContactInfo, 
	   CASE
			WHEN s.Fax IS NOT NULL THEN 'Fax'
			WHEN s.Phone IS NOT NULL THEN 'Phone'
			ELSE N'Không có'
		END AS ContactType
From OrderItem oi
JOIN Product p ON oi.ProductId = p.Id
JOIN Supplier s ON p.SupplierId = s.Id
SET STATISTICS TIME OFF;

/* Bài 4
Cho biết Id của database Northwind, Id của bảng Supplier, Id của User mà bạn đang đăng nhập 
là bao nhiêu. Cho biết luôn tên User mà đang đăng nhập
*/

SET STATISTICS TIME ON;
SELECT DB_ID('Northwind') AS NorthwindDatabaseId;
SELECT OBJECT_ID('Supplier') AS SupplierTableId;
SELECT USER_ID() AS CurrentUserId, SYSTEM_USER AS CurrentUserName;
SET STATISTICS TIME OFF;


/* Bài 5
Cho biết các thông tin user_update, user_seek, user_scan và user_lookup trên bảng Order trong 
database Northwind
*/

SET STATISTICS TIME ON;
SELECT 
    OBJECT_NAME(s.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    user_seeks,
    user_scans,
    user_lookups,
    user_updates
FROM sys.dm_db_index_usage_stats s
JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id
WHERE OBJECT_NAME(s.object_id) = 'Order'
  AND s.database_id = DB_ID('Northwind');
SET STATISTICS TIME OFF;


/* Bài 6
Dùng WITH phân chia cây như sau : Mức 0 là các Quốc Gia(Country), mức 1 là các Thành Phố 
(City) thuộc Country đó, và mức 2 là các Hóa Đơn (Order) thuộc khách hàng từ Country-City đó
*/

SET STATISTICS TIME ON;
WITH OrderHierarchy(Country, City, OrderInfo, Level)
AS (
    SELECT DISTINCT Country,
           City = CAST('' AS NVARCHAR(255)),
           OrderInfo = CAST('' AS NVARCHAR(255)),
           Level = 0
    FROM Customer
    WHERE Country IS NOT NULL
    
    UNION ALL

    SELECT c.Country,
           City = CAST(c.City AS NVARCHAR(255)),
           OrderInfo = CAST('' AS NVARCHAR(255)),
           Level = oh.Level + 1
    FROM OrderHierarchy oh
    INNER JOIN Customer c ON oh.Country = c.Country
    WHERE oh.Level = 0 AND c.City IS NOT NULL
    
    UNION ALL
    
    SELECT c.Country,
		   City = CAST(c.City AS NVARCHAR(255)),
		   OrderInfo = CAST('Hóa đơn #' + o.OrderNumber + ' (' + CONVERT(VARCHAR, o.OrderDate, 103) + ')' AS NVARCHAR(255)),
		   Level = oh.Level + 1
    FROM OrderHierarchy oh
    INNER JOIN Customer c ON oh.Country = c.Country AND oh.City = c.City
    INNER JOIN [Order] o ON c.Id = o.CustomerId
    WHERE oh.Level = 1
)
SELECT 
    [Quốc Gia] = CASE WHEN Level = 0 THEN Country ELSE '--' END,
    [Thành Phố] = CASE WHEN Level = 1 THEN City ELSE '---' END,
    [Hóa Đơn] = CASE WHEN Level = 2 THEN OrderInfo ELSE '' END,
    [Cấp] = Level
FROM OrderHierarchy
ORDER BY Country, City, Level;
SET STATISTICS TIME OFF;

/* Bài 7
Xuất những hóa đơn từ khách hàng France mà có tổng số lượng Quantity lớn hơn 50 của các sản 
phẩm thuộc hóa đơn ấy 
*/

SET STATISTICS TIME ON;
SELECT 
    o.Id AS OrderId, o.OrderNumber,
    CONCAT(c.FirstName, ' ', c.LastName) AS FullName,
    c.Country, SUM(oi.Quantity) AS TotalQuantity
FROM "Order" o
	JOIN OrderItem oi ON o.Id = oi.OrderId
	JOIN Customer c ON o.CustomerId = c.Id
WHERE c.Country = 'France'
GROUP BY o.Id, o.OrderNumber, c.FirstName, c.LastName, c.Country
HAVING SUM(oi.Quantity) > 50
ORDER BY TotalQuantity DESC;
SET STATISTICS TIME OFF;

-- LAB 5
/* Bài 1
Tạo các view sau :
		uvw_DetailProductInOrder với các cột sau OrderId, OrderNumber, OrderDate, ProductId, 
		ProductInfo ( = ProductName + Package. Ví dụ: Chai 10 boxes x 20 bags), UnitPrice 
		và Quantity

		uvw_AllProductInOrder với các cột sau OrderId, OrderNumber, OrderDate, ProductList 
		(ví dụ “11,42,72” với OrderId 1), và TotalAmount ( = SUM(UnitPrice * Quantity)) 
		theo mỗi OrderId  (Gợi ý dùng FOR XML PATH để tạo cột ProductList)
*/

SET STATISTICS TIME ON;

GO
CREATE VIEW uvw_DetailProductInOrder
AS
	SELECT oi.OrderId, o.OrderNumber, o.OrderDate, oi.ProductId,
		   p.ProductName + ' x ' + p.Package AS ProductInfo,
		   p.UnitPrice, oi.Quantity
	FROM "Order" o
	JOIN OrderItem oi ON o.Id = oi.OrderId
	JOIN Product p ON oi.ProductId = p.Id
go

SET STATISTICS TIME OFF;


SET STATISTICS TIME ON;
go
CREATE VIEW uvw_AllProductInOrder
AS
SELECT 
    o.Id AS OrderId,
    o.OrderNumber,
    o.OrderDate,
    STUFF((
        SELECT ',' + CAST(oi.ProductId AS VARCHAR(10))
        FROM OrderItem oi
        WHERE oi.OrderId = o.Id
        FOR XML PATH('')
    ), 1, 1, '') AS ProductList,
    SUM(oi.UnitPrice * oi.Quantity) AS TotalAmount
FROM "Order" o
JOIN OrderItem oi ON o.Id = oi.OrderId
GROUP BY o.Id, o.OrderNumber, o.OrderDate;
GO
SET STATISTICS TIME OFF;

/* Bài 2
Dùng view “uvw_DetailProductInOrder“ truy vấn những thông tin có OrderDate trong tháng 7 
*/

SET STATISTICS TIME ON;
SELECT * FROM uvw_DetailProductInOrder WHERE Month(OrderDate) = 7
SET STATISTICS TIME OFF;

/* Bài 3
Dùng view “uvw_AllProductInOrder” truy vấn những hóa đơn Order có ít nhất 3 product trở lên
*/

SET STATISTICS TIME ON;
SELECT * FROM uvw_AllProductInOrder
WHERE LEN(ProductList) - LEN(REPLACE(ProductList, ',', '')) >= 2;
SET STATISTICS TIME OFF;

/* Bài 4
Hai view trên đã readonly chưa ? Có những cách nào làm hai view trên thành readonly ?
Cả uvw_DetailProductInOrder và uvw_AllProductInOrder đều là read-only vì:
Chứa dữ liệu tổng hợp (SUM, COUNT)
Sử dụng JOIN nhiều bảng
Có biểu thức phức tạp (STUFF + FOR XML PATH)
*/


/* Bài 5
Thống kê về thời gian thực thi khi gọi hai view trên. View nào chạy nhanh hơn ? 
*/

SET STATISTICS TIME ON;
SELECT * FROM uvw_DetailProductInOrder;
SET STATISTICS TIME OFF;

-- Đo thời gian cho uvw_AllProductInOrder
SET STATISTICS TIME ON;
SELECT * FROM uvw_AllProductInOrder;
SET STATISTICS TIME OFF;

/* ===== ============================================*/

select * from uvw_DetailProductInOrder

go
Create view OrderItemCustomerr
as
select o.CustomerId as CustomerId, Concat(c.FirstName, ' ', c.LastName) as FullName, o.TotalAmount
From "Order" o
JOIN Customer c on o.CustomerId = c.Id

Group by o.CustomerId, o.TotalAmount, c.FirstName, c.LastName

select * from OrderItemCustomer

go
create view OrderItemView
as
select TotalAmount
From "Order"

select * from OrderItemView

update OrderItemView
set TotalAmount = TotalAmount + 1
where TotalAmount >= 440


drop view OrderItemCustomer

select * from OrderItem


create function dbo.fuGetCurrYear()
Returns int
as
Begin
	return Year(getdate())
end

select dbo.fuGetCurrYear() as "CurrentYear"

select * from "Order"
where year(OrderDate) <= dbo.fuGetCurrYear()


create function dbo.fuDaysInMonth(@Thang int, @Nam int)
returns int
as
begin
	declare @Ngay int
	if @Thang = 2
		begin
			if((@Nam % 4 = 0 and @Nam % 100 <> 0) Or (@Nam%400=0))
				set @Ngay=29
			else
				set @Ngay=28
		end
	else 
		select @Ngay = case when @Thang in (1,3,5,7,8,10,12) then 31 else 30 end
	return @Ngay
end

select dbo.fuDaysInMonth(1,2004) as NumberOfDay


create function fuThu(@ngay Datetime)
returns Nvarchar(10)
as
begin
	declare @kq nvarchar(10)
	select @kq = case datepart(WEEKDAY,@ngay)
		when 1 then N'Chủ nhật'
		when 2 then	N'Thứ 2'
		when 3 then	N'Thứ 3'
		when 4 then	N'Thứ 4'
		when 5 then	N'Thứ 5'
		when 6 then	N'Thứ 6'
		else N'Thứ 7'
		end
	return @kq
end

select dbo.fuThu('2025-04-24') as kq


create function dbo.XemSv(@c NVARCHAR(50))
returns table
as
return (
	
	select FirstName, City
	from Customer
	where Country = @c
)

select * from dbo.XemSv('UK')
drop dbo.XemSv
EXEC sp_helptext 'dbo.XemSv';

IF OBJECT_ID('dbo.XemSv', 'IF') IS NOT NULL
    DROP FUNCTION dbo.XemSv;
GO

-- Hàm xuất danh sách các khách hàng và tổng hóa đơn của khách hàng đó
create function sumOrderBYCus(@maKH int)
returns @resultTable Table
		(
			CustomerId int,
			CustomerName nvarchar(50),
			NumberOrder int
		)
as
begin
	insert into @resultTable
	select c.Id, CustomerName = c.FirstName + ' ' + c.LastName,
	NumberOrder = Count(o.OrderNumber)
	From Customer c
	Join "Order" o on c.Id = o.CustomerId
	Group by c.Id, c.FirstName, c.LastName
	Having c.Id = @maKH Or @maKH = 0

	return 
end


select * from sumOrderBYCus(0)

create  proc sp_AddNewOrder
	@OrderDate datetime,
	@OrderNumber nvarchar(50),
	@CustomerId int,
	@TotalAmount decimal(12,2)
as
begin
	insert into [Order] (OrderDate, OrderNumber, CustomerId, TotalAmount)
	values (@OrderDate,@OrderNumber,@CustomerId,@TotalAmount);
	select SCOPE_IDENTITY() as NewOrderId;
end
go
	
EXEC sp_AddNewOrder 
    @OrderDate = '2012-07-04 00:00:00.000',
    @OrderNumber = '542378',
    @CustomerId = 85,
    @TotalAmount = 445.00;

--- lấy họ tên khách hàng của
create proc sp_getInfo
	@CustomerId INT
as
begin
	IF not exists (select 1 from Customer where Id = @CustomerId)
	begin
		RAISERROR ('CustomerId không tồn tại.', 16, 1); -- Sửa cú pháp RAISERROR		
		return;
	end;

	select 
		Id, FirstName, LastName, City, Country, Phone
	from Customer
	where id = @CustomerId
end;
go

exec sp_getInfo @CustomerId = 1

drop proc sp_getInfo


alter proc sp_getInfo
	@CustomerId INT
as
begin
	IF not exists (select 1 from Customer where Id = @CustomerId)
	begin
		RAISERROR ('CustomerId không tồn tại.', 16, 1); -- Sửa cú pháp RAISERROR		
		return;
	end;

	select 
		Id, FirstName, City, Country, Phone
	from Customer
	where id = @CustomerId
end;
go

declare @test float
SET @test = 1
exec @test = 1.0
print @test

SELECT @test = 1

-- ================================
	@OrderId int,
	@OrderNumber nvarchar(10),
	@OrderDate datetime,
	@ProductId int,
	@ProductInfo nvarchar(50),
	@UnitPrice decimal(12, 2),
	@Quantity int

go
create proc uvw_DetailProductInOrder 
as
begin
	select oi.OrderId, o.OrderNumber, o.OrderDate,
		   oi.ProductId,
		   p.ProductName + ' x ' + p.Package as ProductInfo,
		   p.UnitPrice, oi.Quantity
	From OrderItem oi
	Join "Order" o on oi.OrderId = o.Id
	Join Product p on oi.ProductId = p.Id
	Group by oi.OrderId, o.OrderNumber, o.OrderDate, oi.ProductId,
		   p.ProductName, p.Package, p.UnitPrice, oi.Quantity
end
exec uvw_DetailProductInOrder

drop proc uvw_DetailProductInOrder

go
create function SumAmount(@CustomerId int)
returns @result Table
	(
		CustomerId int,
		TotalAmount int
	)
as
begin
	insert into @result (CustomerId, TotalAmount)
	select CustomerId, SUM(TotalAmount) as TotalAmount
	from "Order" 
	WHERE CustomerId = @CustomerId
	GROUP BY CustomerId;
	return;
end


select dbo.SumAmout(0) as TotalAmount

create function SumAmout (@CustomerId int)
returns int
as
begin 
	declare @TotalAmount int;
	Select @TotalAmount = sum(TotalAmount)
	From [Order]
	WHERE CustomerId = @CustomerId
	return @TotalAmount
end


create function bai2_1(@price1 int, @price2 int)
returns table 
as
return 
(
	select 
		Id,
        ProductName,
        UnitPrice,
        Package,
        SupplierId,
        IsDiscontinued 
	from Product
	where UnitPrice > @price1 and UnitPrice < @price2
)

select * from  dbo.bai2_1(3,20)

create function bai2(@price1 int, @price2 int)
returns  @resultTable table
	(
		Id int,
        ProductName nvarchar(50),
        UnitPrice int,
        Package nvarchar(50),
        SupplierId int
	)
as
begin 
	insert into @resultTable(Id, ProductName, UnitPrice, Package, SupplierId)
	select 
		Id,
        ProductName,
        UnitPrice,
        Package,
        SupplierId
	from Product
	where UnitPrice > @price1 and UnitPrice < @price2
	return 
end

select * from bai2(3, 20)


create function bai3_inline(@arrMonth nvarchar(50))
returns table
as
return (
	select 
		Id, OrderDate, OrderNumber, CustomerId, TotalAmount
		from "Order"
		where Exists (
			select 1
			from string_split(@arrMonth,';') m
			where datename(month, OrderDate) = trim(m.value)
		)
		group by Id, OrderDate, OrderNumber, CustomerId, TotalAmount
)

select * from dbo.bai3_inline('June;July;August;September')

go
create function bai3_multi(@arrMonth nvarchar(50))
returns @resultTable table
	(
		Id INT,
		OrderNumber NVARCHAR(10),
		OrderDate DATETIME,
		CustomerId INT,
		TotalAmount DECIMAL(12,2)
	)
AS
begin
	-- set @arrMonth = Trim(REPLACE(@arrMonth, ' ',''))
	
	insert into @resultTable

	select Id,
        OrderNumber,
        OrderDate,
        CustomerId,
        TotalAmount
	From [Order]
	where exists (
		select 1
		from string_split(@arrMonth,';') m
		where datename(month, OrderDate) = trim(m.value)
	)
	return
end

select * from bai3_multi('June;July;August;September')

create function bai4(@OrderId int)
returns bit
as
begin
	Declare @ProductCount int;
	Declare @IsValid bit;

	select @ProductCount = Quantity
	From OrderItem
	where OrderId = @OrderId;

	if @ProductCount > 5
		set @IsValid = 0;
	else
		set @IsValid = 1;

	return @IsValid
end

select dbo.bai4(1)

CREATE TRIGGER tr_SimpleOrderInsert
ON "Order"
AFTER INSERT
AS
BEGIN
    PRINT 'A new order has been added!';
END
GO

INSERT INTO "Order" (Id, OrderDate, OrderNumber, CustomerId, TotalAmount)
VALUES (1111, '2025-06-01', '11111', 1, 100.50);

--  ===========================================
-- trigger
go
create trigger dispplay_new_supplier ON Supplier
For insert
as
begin
	select * from Supplier
end

INSERT INTO Supplier (CompanyName, ContactName, ContactTitle, City, Country, Phone, Fax)
VALUES ('XYZ Corp', 'John Doe', 'Manager', 'New York', 'USA', '987654321', NULL);


CREATE TABLE mathang
(
    mahang NVARCHAR(5) PRIMARY KEY, /*mã hàng*/
    tenhang NVARCHAR(50) NOT NULL, /*tên hàng*/
    soluong INT, /*số lượng hàng hiện có*/
)

CREATE TABLE nhatkybanhang
(
    stt INT IDENTITY PRIMARY KEY,
    ngay DATETIME, /*ngày bán hàng*/
    nguoinua NVARCHAR(30), /*tên người mua hàng*/
    mahang NVARCHAR(5) /*mã mặt hàng được bán*/ FOREIGN KEY REFERENCES mathang(mahang),
    soluong INT, /*số lượng hàng được bán*/
    giaban MONEY /*giá bán hàng*/
)

go
create trigger trg_nhatkybanhang_insert on nhatkybanhang
for insert
as
begin
	update mathang
	set mathang.soluong = mathang.soluong - inserted.soluong
	from mathang
	inner join inserted on mathang.mahang = inserted.mahang
end

select * from nhatkybanhang
select * from mathang

INSERT INTO nhatkybanhang (ngay, nguoinua, mahang, soluong, giaban)
VALUES ('2025-04-27', 'Nguyen Van A', 'MH002', 2, 15000000);

INSERT INTO mathang (mahang, tenhang, soluong)
VALUES 
    ('MH002', 'Điện thoại Samsung', 100),
    ('MH003', 'Tai nghe Sony', 200),
    ('MH004', 'Máy in HP', NULL);


create trigger trg_nhatkybanhang_update_soluong on nhatkybanhang
for update
as
	begin
		if update(soluong)
			update mathang
			set mathang.soluong = mathang.soluong -(inserted.soluong - deleted.soluong)

			from ( deleted inner join inserted on deleted.stt = inserted.stt) 
			inner join mathang on mathang.mahang = deleted.mahang
	end

select * from nhatkybanhang
select * from mathang

update nhatkybanhang
set soluong = soluong+20
where stt=2

create trigger trg_mathang_delete on mathang
for delete
as
rollback transaction

delete from  mathang


CREATE TRIGGER trg_prevent_delete_specific_mathang
ON mathang
FOR DELETE
AS
BEGIN
    IF EXISTS (SELECT 1 FROM deleted WHERE mahang = 'MH01')
    BEGIN
        ROLLBACK TRANSACTION;
        RAISERROR(N'Không được phép xóa mặt hàng có mã MH01!', 16, 1);
    END
END

SELECT 1  
FROM mathang
where mahang = 'MH01'


-- ==========================



DEALLOCATE Cursor1
-- ===

declare @TenHang nvarchar(50)
declare Cursor1 cursor for
select tenhang
from mathang

open Cursor1

fetch next from Cursor1 into @TenHang

while @@fetch_status = 0
begin 
	print @TenHang
	fetch next from Cursor1 into @TenHang
end

close Cursor1

deallocate Cursor1

-- lab7
/*
Tạo một trigger để kiểm tra việc khi xóa một Customer thì thông tin Order 
của Customer đó sẽ chuyển về cho CustomerId là 1 
*/

create trigger trigger_delete_customer on customer
for delete
as
begin
	declare @DeleteCustomerID int
	select @DeleteCustomerID = Id 
	from deleted

	update "Order" 
	set CustomerId = 1
	where CustomerId = @DeleteCustomerID
	Print 'Cac hoa don cua kh CustomerId = ' + ltrim(str(@DeleteCustomerID)) + ' da chuyen qua ccho customerID = 1'
end

select * from "Order"
where CustomerId = 1

select * from "Order"
where Id IN (2,191,199,301,361,720)

delete from Customer where Id = 79
alter table "Order" drop constraint fk_order_reference_customer

/*
Tạo một trigger khi xóa CustomerId 1 thì sẽ không cho xóa và báo lỗi : 
“Đây là khách hàng không được xóa” sau đó ROLL BACK lại hành động xóa
*/
go
create trigger trigger_customerID1Delete
ON Customer
For Delete
as
Begin
	declare @Id int
	select @id = Id from deleted 

	if (@Id = 1) 
	begin
		raiserror('Customer Id = 1 thi ban eo dc xoa: ',16,1)
		rollback transaction
	end
end

select * from Customer where Id = 1

create trigger trigger_customerID1Delete
ON Customer
For Delete
as
Begin
	if exists (select 1 from deleted where Id = 1) 
	begin
		raiserror('Customer Id = 1 thi ban eo dc xoa: ',16,1)
		rollback transaction
	end
end

/*
Viết một trigger sau không cho phép cập nhật UnitPrice của Product 
nhỏ hơn hoặc bằng 0. Nếu cập nhật thì sẽ báo lỗi và ROLL BACK lại
*/

create trigger trigger_notupdate_price_lt0 on Product
for update
as
begin
	declare @update_price int
	select @update_price = UnitPrice from inserted

	if(@update_price < 0)
	begin
		raiserror('May dang update gia < 0 do',16,1)
		rollback transaction
	end
end

select * from Product where Id =1

update Product
set UnitPrice =-1
where Id = 1

/*
Viết ví dụ sử dụng CURSOR để duyệt dữ liệu
Viết một Function với input là tiêu chuẩn dạng Package và sau đó dựa trên tiêu chuẩn này xuất ra danh sách các Id và ProductName như sau
*/

go
create function ufn_ListProductByPackage (@PackageDes nvarchar(max))
returns nvarchar(max)
as
BEGIN
	declare @ProductList Nvarchar(max) = @PackageDes + ' list is '
	declare @Id int
	declare @ProductName nvarchar(50)
	declare ProductCursor Cursor read_only
	for 
		select Id, ProductName
		From Product
		where Package like '%' + @PackageDes +'%'

	Open ProductCursor

	fetch next from ProductCursor into @Id, @ProductName

	while @@FETCH_STATUS = 0
	begin
		set @ProductList = @ProductList + ltrim(str(@Id)) + ':' + @ProductName + ' ; '
		fetch next from ProductCursor into @Id, @ProductName
	end

	close ProductCursor	
	deallocate ProductCursor

	return @ProductList
END

select * from Product
select dbo.ufn_ListProductByPackage('boxes')
/*
Viết một giao dịch cập nhật UnitPrice của tất cả các sản phẩm có xuất sứ từ USA bằng 
cách input vào một cơ số @DFactor và tính UnitPrice mới theo công thức 
UnitPrice = UnitPrice / @DFactor. Sau đó cho biết có bao nhiêu sản phẩm đã được cập nhật
UnitPrice. Dùng TRANSACTION trong trường hợp này để kiểm soát lỗi có thể xảy ra trong 
quá trình cập nhật và ROLL BACK khi cần.

*/

begin try
	begin transaction UpdatePriceTrans

		declare @CountUpdatePrice int = 0
		declare @DFactor int;
		set @DFactor  = 2

		update P
		set UnitPrice = UnitPrice / @DFactor
		From Product P
		inner join Supplier s on P.SupplierId = s.Id
		where s.Country like '%USA%'

		set @CountUpdatePrice = @@rowcount
		Print 'Cap nhat thanh cong' + ltrim(rtrim(str(@CountUpdatePrice)))

	Commit transaction UpdatePriceTrans

end try
begin catch
	rollback Transaction UpdatePriceTrans
	print 'Cap nhat that bai. '
	print error_message();
end catch

select * from Product where Id = 3

select * from Supplier

-- LAB 7 – HQTCSDL – Trigger-Transaction-Cursor-Temp Table
/*
Viết trigger khi xóa một OrderId thì xóa luôn các thông tin của Order đó trong bảng 
OrderItem. Nếu có Foreign Key 
Constraint xảy ra không cho xóa thì hãy xóa Foreign Key Constraint đó đi rồi thực thi. 
*/
alter table "Order"
add constraint fk_order_reference_customer
foreign key (CustomerId) references Customer(Id)

alter table "OrderItem"
add constraint fk_orderitem_reference_order
foreign key (OrderId) references "Order"(Id)


alter table "Order" 
drop constraint fk_order_reference_customer

alter table OrderItem
drop constraint fk_orderitem_reference_order

GO
create trigger trigger_del_Order on "Order"
for delete
as
BEGIN
	declare @OrderId int

	select @OrderId = Id from deleted

	delete from "Order"
	where Id = @OrderId

	delete from OrderItem
	where OrderId = @OrderId
END

DELETE FROM "Order" WHERE CustomerId = 85;
select * from "Order" WHERE Id = 1;
select * FROM "OrderItem" WHERE "OrderId" = 1;

SELECT * 
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
WHERE TABLE_NAME = 'Order';
/*
Viết trigger khi xóa hóa đơn của khách hàng Id = 1 thì báo lỗi không cho xóa sau đó ROLL BACK lại. Lưu ý: Đưa trigger 
này lên làm Trigger đầu tiên thực thi xóa dữ liệu trên bảng Order
*/
create trigger delete_order_ID1 on "Order"
for delete
as
begin
	declare @CustomerId int
	select @CustomerId = CustomerId from deleted

	if @CustomerId = 1
	begin
		raiserror('Khong dc xoa don hang cua customer id = 1',16,1);
		rollback transaction
	end
end

delete from "Order" where CustomerId = 1

/*
Viết trigger không cho phép cập nhật Phone là NULL hay trong Phone có chữ cái ở bảng Supplier. Nếu có thì báo lỗi 
và ROLL BACK lại
*/

create trigger trg_notUpdate_phone_Null on Supplier
for update
as
begin
	SET NOCOUNT ON;

	declare @SupplierId int
	declare @Phone nvarchar(50)
	select @SupplierId = Id, @Phone= Phone  from inserted

	if @Phone is null
	begin
		raiserror('khong dc cap nhat phone = null ',16,1)
		rollback transaction
	end
end

select * from Supplier
update Supplier
set Phone = null
where Id in (1,2,3)

/*
Viết một function với input vào Country và xuất ra danh sách các Id và Company 
Name ở thành phố đó theo dạng sau 
*/
go
create function fnc_Country_SUp (@Country nvarchar(max))
returns nvarchar(max)
as
begin
	declare @CompanyName nvarchar(100)
	declare @Id int
	declare @SupplierList nvarchar(max) = 'Companies in ' + @Country + ' are: '

	declare Cur1 Cursor for
	select CompanyName, Id  
	from Supplier
	where Country = @Country

	open Cur1

	fetch next from Cur1 into @CompanyName,@Id

	while @@FETCH_STATUS = 0
	begin
		set @SupplierList = @SupplierList + @CompanyName + '(ID:' + ltrim(rtrim(str(@Id))) + ');'

		fetch next from Cur1 into  @CompanyName,@Id
	end

	close Cur1
	deallocate Cur1

	return @SupplierList
end

select * from Supplier
select dbo.fnc_Country_SUp('USA')


begin try
	begin transaction UpdateQua
		declare @DFactor int
		declare @CountUpdate int = 0;
		set @DFactor = 2

		update OrderItem
		set Quantity = Quantity / @DFactor
		from OrderItem
		join "Order" on OrderItem.OrderId = "Order".Id
		join Customer on "Order".CustomerId = Customer.Id
		where Customer.Country = 'USA'
		
		set @CountUpdate = @@rowcount
		print 'Cap nhat thanh cong: ' + ltrim(rtrim(str(@CountUpdate)))
end try
begin catch
	Print 'rollback'
	rollback transaction UpdateQua
end catch

select * from OrderItem
select oi.Quantity, c.Country
from OrderItem oi
join "Order" on oi.OrderId = "Order".Id
join Customer c on "Order".CustomerId = c.Id
where c.Country = 'USA'

-- lab 8
/*
Viết một stored procedure với Input là một mã khách hàng CustomerId và Output là 
một hóa đơn OrderId của khách hàng đó có Total Amount là nhỏ nhất và một hóa đơn 
OrderId của khách hàng đó có Total Amount là lớn nhất 
*/
go
create proc prc_lab8_bai1 
	@CustomerId int
as
begin
	select TOP 1 Id as MaxOrderId, TotalAmount as MaxTotalAmount
	From "Order"
	where CustomerId = @CustomerId
	Order by TotalAmount DESC

	select TOP 1 Id as MinOrderId, TotalAmount as MaxTotalAmount
	From "Order"
	where CustomerId = @CustomerId
	Order by TotalAmount ASC
end

select * from "Order"

exec prc_lab8_bai1 
	@CustomerId = 5

	CREATE PROCEDURE usp_GetOrderID_CustomerID_MaxAndMinTotalQuantity
    @CustomerId INT,
    @MaxOrderId INT OUTPUT,
    @MaxTotalAmount DECIMAL(18,2) OUTPUT,
    @MinOrderId INT OUTPUT,
    @MinTotalAmount DECIMAL(18,2) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- Hóa đơn có TotalAmount lớn nhất
    SELECT TOP 1
        @MaxOrderId = Id,
        @MaxTotalAmount = TotalAmount
    FROM "Order"
    WHERE CustomerId = @CustomerId
    ORDER BY TotalAmount DESC;

    -- Hóa đơn có TotalAmount nhỏ nhất
    SELECT TOP 1
        @MinOrderId = Id,
        @MinTotalAmount = TotalAmount
    FROM "Order"
    WHERE CustomerId = @CustomerId
    ORDER BY TotalAmount ASC;
END

DECLARE 
    @MaxOrderId INT,
    @MaxTotalAmount DECIMAL(18,2),
    @MinOrderId INT,
    @MinTotalAmount DECIMAL(18,2);

EXEC usp_GetOrderID_CustomerID_MaxAndMinTotalQuantity 
    @CustomerId = 5,
    @MaxOrderId = @MaxOrderId OUTPUT,
    @MaxTotalAmount = @MaxTotalAmount OUTPUT,
    @MinOrderId = @MinOrderId OUTPUT,
    @MinTotalAmount = @MinTotalAmount OUTPUT;

-- In kết quả
SELECT 
    @MaxOrderId AS MaxOrderId,
    @MaxTotalAmount AS MaxTotalAmount,
    @MinOrderId AS MinOrderId,
    @MinTotalAmount AS MinTotalAmount;



create proc prc_lab8_bai1_1
	@CustomerId int
as
begin
	with MaxOrder as (
		select top 1 Id as MaxOrderId, TotalAmount as MaxTotalAmount
		from "Order"
		where CustomerId = @CustomerId
		Order by TotalAmount DESC
	), 
	MinOrder as (
		select top 1 Id as MinOrderId, TotalAmount as MinTotalAmount
			from "Order"
			where CustomerId = @CustomerId
			Order by TotalAmount ASC
	)
	select MaxOrder.MaxOrderId,
		MaxOrder.MaxTotalAmount,
		MinOrder.MinOrderId,
		MinOrder.MinTotalAmount
	From MaxOrder
	Cross join MinOrder
end

exec prc_lab8_bai1_1
	@CustomerId = 5
/*
Viết một stored procedure để thêm vào một Customer với Input là FirstName, LastName, 
City, Country, và Phone. Lưu ý nếu các input mà rỗng hoặc Input đó đã có trong bảng 
thì báo lỗi tương ứng và ROLL BACK lại
*/
create proc prc_insert_Cus
	@FirstName nvarchar(40),
	@LastName nvarchar(40),
	@City nvarchar(40),
	@Country nvarchar(40),
	@Phone nvarchar(40)
as
begin
	insert into Customer(FirstName, LastName, City, Country,  Phone )
	values (@FirstName, @LastName, @City, @Country,  @Phone )
end

exec prc_insert_Cus
	@FirstName = 'Toan',
	@LastName = 'Pham',
	@City = 'Ha Noi',
	@Country = 'Viet Nam',
	@Phone = '0932682977'

select * from Customer
/*
Viết Store Procedure cập nhật lại UnitPrice của sản phẩm trong bảng OrderItem. Khi 
cập nhật lại UnitPrice này thì cũng phải cập nhật lại Total Amount trong bảng Order 
tương ứng với Total Amount = SUM (UnitPrice *Quantity)
*/

alter proc proc_updateUnitPric 
	@OrderItemId int,
	@UnitPrice int
as
begin
	declare @OrderId int

	select @OrderId = OrderId
	from OrderItem
	where Id = @OrderItemId

	update OrderItem
	set UnitPrice = @UnitPrice
	where Id = @OrderItemId

	Update "Order"
	set TotalAmount = (
		SELECT SUM(UnitPrice * Quantity)
		from OrderItem
		where OrderId = @OrderId
	)
	where Id = @OrderId
end

exec proc_updateUnitPric
	@OrderItemId = 6 ,
	@UnitPrice = 9

select * from OrderItem
where OrderId = 3

select * from "Order"
where Id = 3

select OrderId
from OrderItem
where Id = 6