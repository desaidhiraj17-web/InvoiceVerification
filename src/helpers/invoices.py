from enum import Enum
from typing import Dict
from sqlalchemy import or_, text
from fastapi import HTTPException
from src.logger.logger_setup import logger
from datetime import datetime,timedelta
import calendar

class FileUploadType(str, Enum):
    invoice = "invoice"
    party_master = "party_master"
    product_master = "product_master"
    rack_master = "rack_master"
    tray_master = "tray_master"
     
    
def list_invoices_base_query():
    base_query = """
        SELECT 
            i.id AS invoice_id,
            i.invoice_no,
            i.invoice_date,
            i.priority,
            i.status,

            
            p.id,
            p.party_code,
            p.party_name,
            p.active AS party_active

        FROM invoices i
        JOIN party_master p ON p.id = i.party_id

    """
    return base_query


def invoices_return_structure(rows):
    try:
        invoice_map: Dict[str, Dict] = {}

        for row in rows:
            inv_id = row["invoice_id"]

            # Create invoice if not already added
            if inv_id not in invoice_map:
                invoice_map[inv_id] = {
                    "id": inv_id,
                    "invoice_no": row["invoice_no"],
                    "invoice_date": row["invoice_date"],
                    "priority": row["priority"],
                    "status": row["status"],
                    "party": {
                        "party_code": row["party_code"],
                        "party_name": row["party_name"],
                        "active": row["party_active"],
                    },

                }
        return invoice_map
    except Exception as e:
        logger.exception(f"invoices_return_structure {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


def list_invoices_products_base_query(rack_no: str | None = None):
    base_query = """
        SELECT 
            ip.id AS product_id,
            ip.product_name,
            ip.batch_number,
            ip.expiry_date,
            ip.mrp,
            ip.actual_qty,
            ip.scanned_qty,
            ip.rack_no,
            ip.scan_status,
            shipper_val AS shipper_uom,
            box_val AS box_uom,
            strip_val AS strip_uom,
            pm.division,
            pm.barcode1,
            pm.barcode2,
            pm.optional1,
            pm.optional2
        FROM invoice_product_list ip
        JOIN invoices inv ON ip.invoice_id = inv.id
        LEFT JOIN product_qty_converter pqc on ip.product_name = pqc.product_name
        LEFT JOIN product_master pm
            ON ip.product_name = pm.product_name
            AND ip.batch_number = pm.batch_number
            AND ip.expiry_date = pm.expiry_date
            AND ip.mrp = pm.mrp
        WHERE ip.invoice_id = :invoice_id
    """
    
    if rack_no:
        base_query += " AND ip.rack_no = :rack_no"
    
    return base_query


async def check_invoice_exists(db, invoice_id: str) -> bool:
    try:
        query = """
            SELECT 1
            FROM invoices
            WHERE id = :invoice_id
            LIMIT 1
        """
        result = await db.execute(text(query), {"invoice_id": invoice_id})
        row = result.first()
        return row is not None
    except Exception as e:
        logger.exception(f"inside check_invoice_exists: {e}")
        raise HTTPException(status_code=404, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


async def check_duplicate_csv_product_master(rows):
    try:
        seen = set()
        for row in rows:
            key = (
                row["product_name"].strip(),
                row["batch_number"].strip(),
                row["expiry_date"].strip(),
                float(row["mrp"])
            )
            if key in seen:
                logger.exception(f"duplicate entries found in csv {key}")
                raise HTTPException(status_code=400, detail={"status":"error",
                    "message":f"duplicate entries found in csv for {key}"})     
            seen.add(key)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"inside : check_duplicate_csv_product_master {e}")
        raise HTTPException(status_code=404, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        
        
def parse_expiry_or_mfg_date(date_str: str,date_type:str) -> str:
    """Parses 'Nov-24' or '2025-10-31' → returns formatted date string."""
    try:
        date_str = date_str.strip()
        # Handle "Nov-24" (month abbreviation + 2-digit year)
        if "-" in date_str and len(date_str.split("-")[1]) == 2:
            parsed_date = datetime.strptime(date_str, "%b-%y")

        # Handle "11-2025" (MM-YYYY)
        elif "-" in date_str and len(date_str.split("-")[1]) == 4:
            parsed_date = datetime.strptime(date_str, "%b-%Y") if date_str[:3].isalpha() else datetime.strptime(date_str, "%m-%Y")

        # Handle full date (if given)
        else:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
            
        if date_type.lower() == "mfg":
            parsed_date = parsed_date.replace(day=1)
        elif date_type.lower() == "expiry":
            last_day = calendar.monthrange(parsed_date.year, parsed_date.month)[1]
            parsed_date = parsed_date.replace(day=last_day)

        return parsed_date.strftime("%d-%m-%Y")

    except Exception as e:
        logger.warning(f" Could not parse date '{date_str}', using current date instead.")
        return date_str
        
        
def invoice_upload_date_format(date_str: str) -> str:
    try:
        date_str = date_str.strip()
        return date_str.replace("/","-")
        
    except Exception as e:
        # Default fallback (log & return current time)
        logger.warning(f" Could not parse date '{date_str}', using current date instead.")
        return date_str
    
    
update_invoice_status_base_query = """
                    UPDATE invoices
                    SET status = 'checked',
                        updated_at = :updated_at
                    WHERE id = :invoice_id
                """
                
invoice_product_exist="""
                SELECT id, scanned_qty FROM invoice_product_list
                WHERE invoice_id = :invoice_id
                    AND batch_number = :batch_number
                    AND expiry_date = :expiry_date
                    AND mrp = :mrp
            """
            
invoice_product_list_insert_query="""
        INSERT INTO invoice_product_list (
            id, invoice_id, product_name, batch_number, expiry_date, mrp, actual_qty, scanned_qty
        ) VALUES (
            :id, :invoice_id, :product_name, :batch_number, :expiry_date, :mrp, :actual_qty, :scanned_qty
        )
        """
        
async def update_invoice_status(db,invoice_id,status,now_str):
    try:

        update_invoice_status_query = """
            UPDATE invoices
            SET status = :status,
                updated_at = :updated_at
            WHERE id = :invoice_id
        """

        await db.execute(
            text(update_invoice_status_query),
            {
                "status": status,
                "updated_at": now_str,
                "invoice_id": invoice_id
            }
        )
        logger.info(f"Invoice {invoice_id} status updated to {status}")
    except Exception as e:
        logger.exception(f"Inside scan_quantity_update_products: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
def epoch_to_str(epoch_value: int) -> str:
    """
    Converts epoch timestamp (in seconds or milliseconds)
    into a human-readable datetime string.
    """
    # Handle None or zero safely
    if not epoch_value:
        return None

    # Detect if it's milliseconds (larger than year ~2286)
    if epoch_value > 1e11:  # e.g. 1763009471000
        epoch_value = epoch_value / 1000

    return datetime.fromtimestamp(epoch_value).strftime("%d-%m-%Y %H:%M:%S")


async def check_invoice_metadata_fields_exist(data):
    try:
        input_fields = {
                k: v for k, v in data.model_dump(exclude_unset=True).items() if k != "status"
            }

            # Case A: No fields sent
        if not input_fields:
            logger.exception("No fields provided to update")
            raise HTTPException(
                status_code=400,
                detail={"status": "error", "message": "No fields provided to update"},
            )
        return input_fields
    except Exception as e:
        logger.exception(f"Inside check_invoice_metadata_fields_exist function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        

invoices_metadata_field_map = {
            "picker_start":"picker_id",
            "picker_end":"picker_id",
            "checker_start":"checker_id",
            "checker_end":"checker_id",
            "packer_start":"packer_id",
            "packer_end":"packer_id"
        }

async def invoice_metadata_row_exists(db,invoice_id):
    try:
        # # Step 3: Check if invoice_metadata row already exists
        check_query = "SELECT * FROM invoice_metadata WHERE invoice_id = :invoice_id"
        result = await db.execute(text(check_query), {"invoice_id": invoice_id})
        existing_row = result.mappings().first()
        return existing_row
    except Exception as e:
        logger.exception(f"Inside metadata_row_exists api: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        
async def check_invoice_product_exists(data,db):
    try:
        requested_ids = [item.invoice_product_id for item in data.products 
                        if item.invoice_product_id not in (None, "", " ")]
        if not requested_ids:
            return

        # Dynamically create placeholders
        placeholders = ", ".join([f":id{i}" for i in range(len(requested_ids))])

        # Build raw SQL
        query = text(f"""
            SELECT id FROM invoice_product_list 
            WHERE id IN ({placeholders})
        """)

        # Build params dictionary
        params = {f"id{i}": pid for i, pid in enumerate(requested_ids)}

        # Execute
        result = await db.execute(query, params)

        existing_ids = {row[0] for row in result.fetchall()}

        # Detect invalid IDs
        invalid_ids = [pid for pid in requested_ids if pid not in existing_ids]

        if invalid_ids:
            logger.error(f"Invalid invoice_product_id(s): {invalid_ids}")
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "error",
                    "message": f"Invalid invoice_product_id(s): {invalid_ids}"
                }
            )

    except HTTPException:
            raise
    except Exception as e:
        logger.exception(f"inside check_invoice_product_existss: {e}")
        raise HTTPException(status_code=404, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})