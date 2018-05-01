-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema sls_tran_sch1
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema sls_tran_sch1
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `sls_tran_sch1` DEFAULT CHARACTER SET utf8 ;
USE `sls_tran_sch1` ;

-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`DateDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`DateDim` (
  `DDK` INT NOT NULL AUTO_INCREMENT,
  `Date` DATETIME NULL,
  `Year_int` INT(4) NULL,
  `Month_int` INT(2) NULL,
  `Month_abbr` CHAR(3) NULL,
  `Day_int` INT(2) NULL,
  `DayOfWeek_int` INT(1) NULL,
  `DayOfWeek_char` CHAR(10) NULL,
  `DayOfYear_int` INT(3) NULL,
  PRIMARY KEY (`DDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`TimeDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`TimeDim` (
  `TDK` INT NOT NULL AUTO_INCREMENT,
  `Time` TIME NULL,
  `Hour_24_int` INT(2) NULL,
  `Minute_int` INT(2) NULL,
  `Second_int` INT(2) NULL,
  `Hour_12_int` INT(2) NULL,
  `AM_PM_char` CHAR(2) NULL,
  PRIMARY KEY (`TDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`ItemListDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`ItemListDim` (
  `ILDK` INT NOT NULL AUTO_INCREMENT,
  `UPC` CHAR(20) NULL,
  `ItemID` INT NULL,
  `LongDes` CHAR(80) NULL,
  `ShortDes` CHAR(80) NULL,
  `ExtraDes` CHAR(80) NULL,
  PRIMARY KEY (`ILDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`ItemJunkDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`ItemJunkDim` (
  `IJDK` INT NOT NULL AUTO_INCREMENT,
  `StoreBrand` CHAR(2) NULL,
  `Status` CHAR(20) NULL,
  PRIMARY KEY (`IJDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`ItemHierarchyDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`ItemHierarchyDim` (
  `IHDK` INT NOT NULL AUTO_INCREMENT,
  `DepartmentCode` CHAR(15) NULL,
  `FamilyCode` CHAR(15) NULL,
  `FamilyDes` CHAR(50) NULL,
  `CategoryCode` CHAR(15) NULL,
  `CategoryDes` CHAR(50) NULL,
  `ClassCode` CHAR(15) NULL,
  `ClassDes` CHAR(50) NULL,
  PRIMARY KEY (`IHDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`StoreJunkDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`StoreJunkDim` (
  `SJDK` INT NOT NULL AUTO_INCREMENT,
  `StoreNum` INT NULL,
  `StoreName` CHAR(50) NULL,
  `ActiveFlag` CHAR(3) NULL,
  `SqFoot` INT NULL,
  `ClusterName` CHAR(50) NULL,
  PRIMARY KEY (`SJDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`StoreLocationDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`StoreLocationDim` (
  `SLDK` INT NOT NULL AUTO_INCREMENT,
  `Region` CHAR(60) NULL,
  `StateCode` CHAR(10) NULL,
  `City` CHAR(50) NULL,
  `ZipCode` INT(5) NULL,
  `AddressLine1` CHAR(80) NULL,
  PRIMARY KEY (`SLDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`SalesJunkDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`SalesJunkDim` (
  `SJDK` INT NOT NULL AUTO_INCREMENT,
  `Register` INT NULL,
  `DeptNum` INT NULL,
  `CashierNum` INT NULL,
  `PriceType` CHAR(40) NULL,
  `ServiceType` CHAR(40) NULL,
  `TenderType` CHAR(40) NULL,
  PRIMARY KEY (`SJDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`CustomerDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`CustomerDim` (
  `CDK` INT NOT NULL AUTO_INCREMENT,
  `LoyaltyCardNum` INT NULL,
  `HouseholdNum` INT NULL,
  `MemberFavStore` INT NULL,
  `City` CHAR(45) NULL,
  `State` CHAR(20) NULL,
  `ZipCode` CHAR(15) NULL,
  PRIMARY KEY (`CDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`ItemAttributesDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`ItemAttributesDim` (
  `IADK` INT NOT NULL AUTO_INCREMENT,
  `UPC` CHAR(20) NULL,
  `ItemAttributeDes` CHAR(80) NULL,
  `ItemAttributeValue` CHAR(45) NULL,
  `AttributeStartDate` DATETIME NULL,
  `AttributeEndDate` DATETIME NULL,
  PRIMARY KEY (`IADK`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`ItemBridge`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`ItemBridge` (
  `ItemAttribute_IADK` INT NOT NULL,
  `ItemList_ILDK` INT NOT NULL,
  INDEX `fk_ItemBridge_ItemList_idx` (`ItemList_ILDK` ASC),
  INDEX `fk_ItemBridge_ItemAttribute_idx` (`ItemAttribute_IADK` ASC),
  CONSTRAINT `fk_table1_ItemListDim2`
    FOREIGN KEY (`ItemList_ILDK`)
    REFERENCES `sls_tran_sch1`.`ItemListDim` (`ILDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_table1_ItemListAttributes1`
    FOREIGN KEY (`ItemAttribute_IADK`)
    REFERENCES `sls_tran_sch1`.`ItemAttributesDim` (`IADK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`StoreServiceDim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`StoreServiceDim` (
  `SSDK` INT NOT NULL AUTO_INCREMENT,
  `StoreNum` INT NULL,
  `StoreType` CHAR(25) NULL,
  `WesternUnion` CHAR(6) NULL,
  `WellsFargoBank` CHAR(6) NULL,
  `WalkInClinic` CHAR(6) NULL,
  `TicketSales` CHAR(6) NULL,
  `TeamSpiritShop` CHAR(6) NULL,
  `Sushi` CHAR(6) NULL,
  `SaladBar` CHAR(6) NULL,
  `RugDoctor` CHAR(6) NULL,
  `Restaurant` CHAR(6) NULL,
  `RedBox` CHAR(6) NULL,
  `MeatMarket` CHAR(6) NULL,
  `MealsForTwo` CHAR(6) NULL,
  `Lottery` CHAR(6) NULL,
  `LivingWellDept` CHAR(6) NULL,
  `KevaJuice` CHAR(6) NULL,
  `HotDeli` CHAR(6) NULL,
  `HerringNationalBank` CHAR(6) NULL,
  `FullServiceSeafood` CHAR(6) NULL,
  `Floral` CHAR(6) NULL,
  `FirstFinancialBank` CHAR(6) NULL,
  `DishGiftCenter` CHAR(6) NULL,
  `Deli` CHAR(6) NULL,
  `DMVregistration` CHAR(6) NULL,
  `Concierge` CHAR(6) NULL,
  `CoffeeShop` CHAR(6) NULL,
  `ClearTalk` CHAR(6) NULL,
  `CityBank` CHAR(6) NULL,
  `CheckCashing` CHAR(6) NULL,
  `BulkFoods` CHAR(6) NULL,
  `BoarsHead` CHAR(6) NULL,
  `BillPay` CHAR(6) NULL,
  `Bakery` CHAR(6) NULL,
  `AngusBeef` CHAR(6) NULL,
  `AmarilloNationalBank` CHAR(6) NULL,
  `Alcohol` CHAR(6) NULL,
  PRIMARY KEY (`SSDK`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `sls_tran_sch1`.`trans_fact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `sls_tran_sch1`.`trans_fact` (
  `DateDim_DDK` INT NOT NULL,
  `TimeDim_TDK` INT NOT NULL,
  `ItemListDim_ILDK` INT NOT NULL,
  `ItemJunkDim_IJDK` INT NOT NULL,
  `ItemHierarchyDim_IHDK` INT NOT NULL,
  `StoreLocationDim_SLDK` INT NOT NULL,
  `SalesJunkDim_SJDK` INT NOT NULL,
  `CustomerDim_CDK` INT NOT NULL,
  `StoreJunkDim_SJDK` INT NOT NULL,
  `StoreServiceDim_SSDK` INT NOT NULL,
  `BusDate` DATETIME NULL,
  `TransNum` INT NULL,
  `ItemQuantity` DECIMAL(5,2) NULL,
  `WeightAmt` DECIMAL(5,2) NULL,
  `SalesAmt` DECIMAL(5,2) NULL,
  `CostAmt` DECIMAL(5,2) NULL,
  INDEX `fk_table1_DateDim_idx` (`DateDim_DDK` ASC),
  INDEX `fk_table1_TimeDim1_idx` (`TimeDim_TDK` ASC),
  INDEX `fk_table1_ItemListDim1_idx` (`ItemListDim_ILDK` ASC),
  INDEX `fk_table1_ItemJunkDim1_idx` (`ItemJunkDim_IJDK` ASC),
  INDEX `fk_table1_ItemHierarchyDim1_idx` (`ItemHierarchyDim_IHDK` ASC),
  INDEX `fk_table1_StoreLocationDim1_idx` (`StoreLocationDim_SLDK` ASC),
  INDEX `fk_table1_SalesJunkDim1_idx` (`SalesJunkDim_SJDK` ASC),
  INDEX `fk_trans_fact_1_CustomerDim1_idx` (`CustomerDim_CDK` ASC),
  INDEX `fk_trans_fact_StoreJunkDim1_idx` (`StoreJunkDim_SJDK` ASC),
  INDEX `fk_trans_fact_StoreServiceDim1_idx` (`StoreServiceDim_SSDK` ASC),
  CONSTRAINT `fk_table1_DateDim`
    FOREIGN KEY (`DateDim_DDK`)
    REFERENCES `sls_tran_sch1`.`DateDim` (`DDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_table1_TimeDim1`
    FOREIGN KEY (`TimeDim_TDK`)
    REFERENCES `sls_tran_sch1`.`TimeDim` (`TDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_table1_ItemListDim1`
    FOREIGN KEY (`ItemListDim_ILDK`)
    REFERENCES `sls_tran_sch1`.`ItemListDim` (`ILDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_table1_ItemJunkDim1`
    FOREIGN KEY (`ItemJunkDim_IJDK`)
    REFERENCES `sls_tran_sch1`.`ItemJunkDim` (`IJDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_table1_ItemHierarchyDim1`
    FOREIGN KEY (`ItemHierarchyDim_IHDK`)
    REFERENCES `sls_tran_sch1`.`ItemHierarchyDim` (`IHDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_table1_StoreLocationDim1`
    FOREIGN KEY (`StoreLocationDim_SLDK`)
    REFERENCES `sls_tran_sch1`.`StoreLocationDim` (`SLDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_table1_SalesJunkDim1`
    FOREIGN KEY (`SalesJunkDim_SJDK`)
    REFERENCES `sls_tran_sch1`.`SalesJunkDim` (`SJDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_trans_fact_1_CustomerDim1`
    FOREIGN KEY (`CustomerDim_CDK`)
    REFERENCES `sls_tran_sch1`.`CustomerDim` (`CDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_trans_fact_StoreJunkDim1`
    FOREIGN KEY (`StoreJunkDim_SJDK`)
    REFERENCES `sls_tran_sch1`.`StoreJunkDim` (`SJDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_trans_fact_StoreServiceDim1`
    FOREIGN KEY (`StoreServiceDim_SSDK`)
    REFERENCES `sls_tran_sch1`.`StoreServiceDim` (`SSDK`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
