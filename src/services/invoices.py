from fastapi import HTTPException
import csv
import io
from sqlalchemy import or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict
from datetime import datetime
import uuid
from sqlalchemy import select
from src.models.invoices import Invoice, InvoiceStatus
from src.models.parties import PartyMaster
from src.logger.logger_setup import logger
from src.helpers.invoices import parse_expiry_or_mfg_date, invoice_upload_date_format, epoch_to_str, invoices_metadata_field_map
from src.models.invoices import ScanStatusEnum
import statistics
from src.schemas.invoices import InvoiceMetadataUpdateSchema

DATETIME_FORMAT = "%d-%m-%Y %H:%M:%S"

async def read_csv_file(file):
    try:
        if not file.filename.lower().endswith(".csv"):
            logger.error("Only CSV files are allowed")
            raise HTTPException(status_code=400, detail={"message":"Only CSV files are allowed"})

        # Read file content asynchronously
        content = await file.read()

        # Convert bytes → string (UTF-8)
        decoded_content = content.decode("utf-8")

        # Parse CSV
        reader = csv.DictReader(io.StringIO(decoded_content))
        rows = list(reader)

        if not rows:
            logger.error("CSV file is empty")
            raise HTTPException(status_code=400, detail={"message":"CSV file is empty"})
        
        return rows
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"read_csv_file: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        
        

async def delete_invoice_product_list(db,invoice_id):
    try:
        delete_invoice_product_query = "DELETE FROM invoice_product_list WHERE invoice_id = :invoice_id"
        
        await db.execute(text(delete_invoice_product_query), {"invoice_id": invoice_id})
        await db.commit()
    except Exception as e:
        logger.exception(f"delete_invoice_product_list {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
    

async def invoice_product_data_handling(db,product_rows_map,row,invoice_id,expiry_date):
    try:
        mrp = round(float(row.get("mrp") or 0), 2)
        product_name = row.get("product_name").strip()
        batch_number = row.get("batch_number").strip() if row.get("batch_number") else ""
        product_key = (
            invoice_id,
            product_name,
            batch_number,
            expiry_date,
            mrp
        )

        qty = float(row.get("qty") or 0)
        # (1) Already present in CSV → merge qty
        if product_key in product_rows_map:
            product_rows_map[product_key]["actual_qty"] += qty
            return None

        # (2) Already exists in DB → stop import
        existing_check = """
            SELECT id FROM invoice_product_list
            WHERE invoice_id = :invoice_id
            AND product_name = :product_name
            AND batch_number = :batch_number
            AND expiry_date = :expiry_date
            AND mrp = :mrp;
        """

        result = await db.execute(text(existing_check), {
            "invoice_id": invoice_id,
            "product_name": product_name,
            "batch_number": batch_number,
            "expiry_date": expiry_date,
            "mrp": mrp
        })

        if result.fetchone():
            logger.exception(f"Duplicate product exists in DB — ({invoice_id} | {row.get('batch_number')} | {mrp})")
            raise HTTPException(status_code=400, detail={
                "status": "error",
                "message": f"Duplicate product exists in DB — ({invoice_id} | {row.get('batch_number')} | {mrp})"
            })

        # (3) New record → store
        product_data = {
            "id": str(uuid.uuid4()),
            "invoice_id": invoice_id,
            "product_name": product_name,
            "batch_number": batch_number,
            "expiry_date": expiry_date,
            "mrp": mrp,
            "actual_qty": qty,
            "scanned_qty": 0.0,
        }
        product_rows_map[product_key] = product_data
        return product_data
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"inside invoice_product_data_handling: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


async def prepare_invoice_upload_data(db,rows,current_user):
    try:
        party_cache = {}     
        invoice_cache = {}
        
        party_rows = []
        invoice_rows = []
        product_rows = []
        product_rows_map = {}
        
        # Preload existing parties & invoices 
        # existing_parties = await db.execute(select(PartyMaster.party_code, PartyMaster.id))
        # existing_parties = {code: pid for code, pid in existing_parties.all()}
        # Load exisitng parties
        existing_parties_query = """
            SELECT party_code, id
            FROM party_master;
        """
        result = await db.execute(text(existing_parties_query))
        existing_parties = {row.party_code: row.id for row in result.mappings().all()}


        # existing_invoices = await db.execute(select(Invoice.invoice_no, Invoice.id, Invoice.status))
        # existing_invoices = {inv_no: {"id": iid, "status": status} for inv_no, iid, status in existing_invoices.all()}
        # Load existing invoices
        existing_invoices_query = """
            SELECT invoice_no, id, status
            FROM invoices;
        """
        result = await db.execute(text(existing_invoices_query))
        existing_invoices = {
            row.invoice_no: {"id": row.id, "status": row.status}
            for row in result.mappings().all()
        }
        
        for row in rows:
            party_code = row.get("party_id")
            invoice_no = row.get("invoice_no")
            
            invoice_raw_date = row.get("invoice_date")
            invoice_date = invoice_upload_date_format(invoice_raw_date)
            
            invoice_product_raw_expiry_date = row.get("expiry_date")
            invoice_product_expiry_date = invoice_upload_date_format(invoice_product_raw_expiry_date)
            
            # expiry_date = parse_expiry_or_mfg_date(row.get("expiry_date", ""))
            
            if party_code in existing_parties:
                party_id = existing_parties[party_code]
            elif party_code in party_cache:
                party_id = party_cache[party_code]
            # if party_code not in party_cache:
            else:
                party_id = str(uuid.uuid4())
                party_rows.append({
                "id": party_id,
                "party_code": party_code,
                "party_name": row.get("party_name"),
                "active": True,
                "updated_by": current_user.id,
                "created_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"), #  default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S")# default=lambda: datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                })
                party_cache[party_code]=party_id
            # else:
            #     party_id = party_cache[party_code]
            
            
            if invoice_no in existing_invoices:
                invoice_info = existing_invoices[invoice_no]
                allowed_status = [None, "", "not_started"]
                if invoice_info["status"] not in allowed_status:
                    logger.error(f"Invoice {invoice_no} already {invoice_info['status']} — cannot override.")
                    raise HTTPException(status_code=400, detail={"status":"error","message":f"Invoice {invoice_no} already {invoice_info['status']} — cannot override."})

                # If not checked → override
                invoice_id = invoice_info["id"]
                await delete_invoice_product_list(db,invoice_id)
                invoice_rows.append({
                    "id": invoice_id,
                    "invoice_no": invoice_no,
                    # "invoice_date": datetime.strptime(row.get("invoice_date"), "%d/%m/%Y").date(),
                    "invoice_type":"purchase",
                    "invoice_date": invoice_date,
                    "party_id": party_id,
                    "priority": "LOW",
                    "status": "not_started",
                    "created_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "started_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                })
            elif invoice_no in invoice_cache:
                invoice_id = invoice_cache[invoice_no]
            else:
                
                invoice_id = str(uuid.uuid4())
                invoice_rows.append({
                "id": invoice_id,
                "invoice_no": invoice_no,
                # "invoice_date": datetime.strptime(row.get("invoice_date"), "%d/%m/%Y").date(),
                "invoice_type":"purchase",
                "invoice_date": invoice_date,
                "party_id": party_id,
                "priority": "LOW",
                "status": "not_started",
                "created_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "started_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                })
                invoice_cache[invoice_no]=invoice_id
                
            # else:
            #     invoice_id = invoice_cache[invoice_no]
            
            product_data = await invoice_product_data_handling(db,product_rows_map,row,invoice_id,invoice_product_expiry_date)
            if product_data:
                product_rows.append(product_data)
                
        logger.info("prepare_invoice_upload_data function run successfully")
        return (party_rows, invoice_rows, product_rows)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"prepare_invoice_upload_data function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


async def invoices_products_assign_rack_no(db,product_rows):
    try:
        product_match_query = """
                SELECT 
                    product_name, batch_number, expiry_date, mrp, rack_no
                FROM product_master
            """
        result = await db.execute(text(product_match_query))
        product_master_map = {
            (row.product_name.strip().lower(), 
                row.batch_number.strip().lower() if row.batch_number else "", 
                str(row.expiry_date), 
                round(float(row.mrp or 0), 2)
                ): row.rack_no or "0"
            for row in result.mappings().all()
        }

        #  Assign rack_no to matching invoice products
        for pr in product_rows:
            key = (
                pr["product_name"].strip().lower(),
                pr["batch_number"].strip().lower() if pr["batch_number"] else "",
                str(pr["expiry_date"]),
                round(float(pr["mrp"] or 0), 2)
            )
            pr["rack_no"] = product_master_map.get(key, "0")
        return product_rows
    except Exception as e:
        logger.exception(f"in invoices_assign_rack_no function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


async def save_invoice_upload_data(db,party_rows, invoice_rows, product_rows):
    try:
        if party_rows:
            await db.execute(
                text("""
                    INSERT INTO party_master (id, party_code, party_name, active, updated_by, created_at, updated_at)
                    VALUES (:id, :party_code, :party_name, :active, :updated_by, :created_at, :updated_at)
                    ON CONFLICT (party_code, party_name, party_address) DO NOTHING
                """), party_rows
            )

        if invoice_rows:
            await db.execute(
                text("""
                    INSERT INTO invoices (id, invoice_no, invoice_type,invoice_date, party_id, priority, status, created_at, updated_at, started_at)
                    VALUES (:id, :invoice_no, :invoice_type, :invoice_date, :party_id, :priority, :status, :created_at, :updated_at, :started_at)
                    ON CONFLICT (id) DO UPDATE SET
                        invoice_date = EXCLUDED.invoice_date,
                        party_id = EXCLUDED.party_id,
                        priority = EXCLUDED.priority,
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at,
                        created_at = EXCLUDED.created_at,
                        started_at = EXCLUDED.started_at
                """), invoice_rows
            )

        if product_rows:
            product_rows_add = await invoices_products_assign_rack_no(db,product_rows)
            await db.execute(
                text("""
                    INSERT INTO invoice_product_list 
                    (id, invoice_id, product_name, batch_number, expiry_date, mrp, actual_qty, scanned_qty, rack_no)
                    VALUES (:id, :invoice_id, :product_name, :batch_number, :expiry_date, :mrp, :actual_qty, :scanned_qty, :rack_no)
                """), product_rows_add
            )
        await db.commit()
        logger.info("in save_invoice_upload_data function run successfully")
    except Exception as e:
        logger.exception(f"in save_invoice_upload_data function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})



async def invoices_apply_filters_search_pagination(db,base_query,search,priority,page,page_size):
    try:
        filters = []
        params = {}
        
        if search:
            filters.append("(i.invoice_no LIKE :search OR p.party_code LIKE :search OR p.party_name LIKE :search)")
            params["search"] = f"%{search}%"
        
        priority_map = {
                1: "HIGH",
                2: "MEDIUM",
                3: "LOW"
                }
        
        if priority is not None:
            # filters.append("i.priority = :priority")
            # params["priority"] = priority
            try:

                priority_value = priority_map.get(int(priority))
                filters.append("i.priority = :priority")
                params["priority"] = priority_value

            except (ValueError, KeyError):
                logger.error("in apply_filters_search_pagination function: Invalid priority value")
                raise HTTPException(status_code=400, detail={"status":"error","message":"Invalid priority value"})

        query_with_filters = base_query

        if filters:
            query_with_filters += " WHERE " + " AND ".join(filters)
            
        # Count total matching rows
        count_query = f"SELECT COUNT(*) AS total FROM ({query_with_filters}) AS subquery"
        total_result = await db.execute(text(count_query), params)
        total = total_result.scalar_one() 
            
            
        offset = (page - 1) * page_size

        # query_with_filters += " ORDER BY i.priority ASC LIMIT :limit OFFSET :offset"
        query_with_filters += """
            ORDER BY CASE i.priority
                        WHEN 'HIGH' THEN 1
                        WHEN 'MEDIUM' THEN 2
                        WHEN 'LOW' THEN 3
                    END ASC
            LIMIT :limit OFFSET :offset
        """
        params["limit"] = page_size
        params["offset"] = offset
        

        query = text(query_with_filters)
        result = await db.execute(query, params)
        rows = result.mappings().all()
        logger.info("apply_filters_search_pagination function runs successfully")
        return (rows,total)
    
    except Exception as e:
        logger.exception(f"in apply_filters_search_pagination function {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


async def paginate_query(
    db: AsyncSession,
    base_query: str,
    params: dict,
    page: int = 1,
    page_size: int = 10,
    order_by: str = "",
):
    
    try: 
    # Count total
        count_query = f"SELECT COUNT(*) AS total FROM ({base_query}) AS subquery"
        total_result = await db.execute(text(count_query), params)
        total = total_result.scalar_one() or 0

        # Apply pagination
        offset = (page - 1) * page_size
        paginated_query = f"""
            {base_query}
            {order_by}
            LIMIT :limit OFFSET :offset
        """

        params = params.copy()
        params.update({"limit": page_size, "offset": offset})

        # Execute
        result = await db.execute(text(paginated_query), params)
        rows = result.mappings().all()

        logger.info("paginate query runs successfully")
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": rows,
        }

    except Exception as e:
        logger.exception(f"inside paginate_query function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})


async def get_invoice_details(db, invoice_id: str):
    try:
        query = """
            SELECT 
                id AS invoice_id,
                invoice_no,
                invoice_date,
                priority,
                status
            FROM invoices
            WHERE id = :invoice_id
        """
        result = await db.execute(text(query), {"invoice_id": invoice_id})
        return result.mappings().first()
    except Exception as e:
        logger.exception(f"inside get_invoice_details: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
        
        
async def prepare_party_master_data(db, rows,current_user):
    try:
        values_list = []
        for row in rows:
            created_at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            updated_at = created_at
            print("current user",current_user,current_user.id)
            updated_by = current_user.id

            values_list.append({
                "id": str(uuid.uuid4()),
                "party_code": row.get("party_code"),
                "party_name": row.get("party_name"),
                "party_gst":row.get("party_gst"),
                "party_address" : row.get("party_address"),
                "party_city" : row.get("party_city"),
                "active" : 1,  # active
                "updated_by" : updated_by,
                "created_at" : created_at,
                "updated_at" : updated_at
            })

        if not values_list:
            logger.exception("No valid rows to insert")
            raise HTTPException(status_code=400, detail={"message": "No valid rows to insert"})
        return values_list
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"inside prepare_party_master_data: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        

async def save_party_master_data(db,values_list):
    try:
        sql = """
        INSERT INTO party_master (
            id, party_code, party_name, party_gst, party_address, party_city, active, updated_by, created_at, updated_at
        ) VALUES (
            :id, :party_code, :party_name, :party_gst, :party_address, :party_city,
            :active, :updated_by, :created_at, :updated_at
        )
        ON CONFLICT(party_code, party_name, party_address) 
            DO UPDATE SET
            party_gst = excluded.party_gst,
            party_city = excluded.party_city,
            active = excluded.active,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at;
        """
        
        await db.execute(text(sql), values_list)
        await db.commit()
    except Exception as e:
        logger.exception(f"inside save_party_master_data: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
        
async def prepare_product_master_data(rows,current_user):
    try:
        now = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        records = []
        for row in rows:
            expiry_date = parse_expiry_or_mfg_date(row.get("expiry_date", ""),"expiry")
            mfg_date = parse_expiry_or_mfg_date(row.get("mfg_date", ""),"mfg")
            rack_no = row.get("rack_no")
            rack_no = rack_no.strip() if rack_no else "0"
            records.append({
                "id":str(uuid.uuid4()),
                "item_code":row["item_code"].strip(),
                "product_name": row["product_name"].strip(),
                "batch_number": row["batch_number"].strip(),
                # "expiry_date": row["expiry_date"].strip(),
                # "mfg_date": row["mfg_date"].strip(),
                "rack_no": rack_no,
                "expiry_date": expiry_date,
                "mfg_date": mfg_date,
                "mrp": round(float(row.get("mrp") or 0), 2),
                "division": row.get("division", "").strip(),
                "obatch": row.get("obatch", ""),
                "barcode1": row.get("barcode1", ""),
                "barcode2": row.get("barcode2", ""),
                "optional1": row.get("optional1", ""),
                "optional2": row.get("optional2", ""),
                "updated_by": current_user.id,   # <-- from logged-in user
                "created_at": now,
                "updated_at": now
            })
            
        return records
    except Exception as e:
        logger.exception(f"inside prepare_product_master_data: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
        
async def save_product_master_data(db,records):
    try:
        insert_query = text("""
            INSERT INTO product_master (
                id, item_code, product_name, batch_number, expiry_date, mfg_date,
                rack_no, mrp, division, obatch, barcode1, barcode2, optional1, optional2,
                updated_by, created_at, updated_at
            )
            VALUES (
                :id, :item_code, :product_name, :batch_number, :expiry_date, :mfg_date,
                :rack_no, :mrp, :division, :obatch, :barcode1, :barcode2, :optional1, :optional2,
                :updated_by, :created_at, :updated_at
            )
            ON CONFLICT(item_code, batch_number, expiry_date, mrp)
            DO UPDATE SET
                mfg_date = excluded.mfg_date,
                rack_no = excluded.rack_no,
                division = excluded.division,
                obatch = excluded.obatch,
                barcode1 = excluded.barcode1,
                barcode2 = excluded.barcode2,
                optional1 = excluded.optional1,
                optional2 = excluded.optional2,
                updated_by = excluded.updated_by,
                updated_at = excluded.updated_at;
        """)

        await db.execute(insert_query, records)
        await db.commit()

        logger.info(f"Bulk upload completed — total {len(records)} processed")
        
        return {"message": f"{len(records)} products processed successfully"}
    
    except Exception as e:
        logger.exception(f"inside save_product_master_data: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e)},
        )
        
        
# async def check_duplicate_csv_rack_master(rows: list[dict]):
async def check_rack_no_rack_master(rows: list[dict]):
    try:
        # rack_numbers = set()

        for i, row in enumerate(rows, start=1):
            rack_no = (row.get("rack_no") or "").strip()

            # Check if rack_no is missing
            if not rack_no:
                logger.exception(f"Missing 'rack_no' in row {i}. It must be present for all entries.")
                raise HTTPException(
                    status_code=400,
                    detail={"status":"error","message": f"Missing 'rack_no' in row {i}. It must be present for all entries."}
                )

            # # Check for duplicates inside CSV
            # if rack_no in rack_numbers:
            #     logger.exception(f"Duplicate rack_no '{rack_no}' found in CSV (row {i}).")
            #     raise HTTPException(
            #         status_code=400,
            #         detail={"status":"error","message": f"Duplicate rack_no '{rack_no}' found in CSV (row {i+1})."}
            #     )
            # rack_numbers.add(rack_no)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"inside check_rack_no_rack_master: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
async def prepare_rack_master_data(db, rows, current_user):
    try:
        # Fetch all usernames → ids mapping
        result = await db.execute(text("SELECT username, id FROM users"))
        user_map = {row.username: row.id for row in result.mappings().all()}

        prepared_records = []
        for r in rows:
            rack_no = str(r.get("rack_no")).strip()
            username = str(r.get("user_assigned")).strip() if r.get("user_assigned") else None
            rack_name = str(r.get("rack_name")).strip() if r.get("rack_name") else rack_no
            user_id = user_map.get(username)
            
            if not user_id:
                logger.info(f"Username {username} not found for rack_no {rack_no}")

            prepared_records.append({
                "rack_no": rack_no,
                "rack_name": rack_name,  
                "user_assigned": user_id,
                "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            })

        logger.info(f"{len(prepared_records)} rack records prepared.")
        return prepared_records
    except Exception as e:
        logger.exception(f"prepare_rack_master_data: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split('\n')[0][:100]}
        )
        
async def save_rack_master_data(db, records):
    try:
        if not records:
            logger.warning("No records to insert in rack_master")
            return {"message": "No records to insert"}

        await db.execute(text("""
            INSERT INTO rack_master (rack_no, rack_name, user_assigned, updated_at)
            VALUES (:rack_no, :rack_name, :user_assigned, :updated_at)
            ON CONFLICT (rack_no) DO UPDATE SET
                rack_name = EXCLUDED.rack_name,
                user_assigned = EXCLUDED.user_assigned,
                updated_at = EXCLUDED.updated_at;
        """), records)
        await db.commit()

        logger.info("Rack master data inserted/updated successfully.")
        return {"message": "Rack master data inserted/updated successfully."}
    except Exception as e:
        logger.exception(f"save_rack_master_data: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split('\n')[0][:100]}
        )
        
        
async def delete_invoice_product(db,product_id):
    try:
        delete_query = """
            DELETE FROM invoice_product_list
            WHERE id = :product_id 
        """
        result = await db.execute(text(delete_query), {
            "product_id": product_id
        })

        if result.rowcount == 0:
            logger.exception(f"Product id: {product_id} not found in invoice")
            raise HTTPException(
                status_code=404,
                detail={"status": "error", "message": "Product not found in invoice"}
            )

        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside invoice_product_check: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
      
      
async def invoice_product_check(db,invoice_id,data):
    try:
        rounded_mrp = round(float(data.mrp), 2)
        check_query = """
            SELECT id FROM invoice_product_list 
            WHERE invoice_id = :invoice_id
              AND product_name = :product_name
              AND batch_number = :batch_number
              AND expiry_date = :expiry_date
              AND ABS(mrp - :mrp) < 0.01
        """
        result = await db.execute(
            text(check_query),
            {
                "invoice_id": invoice_id,
                "product_name": data.product_name.strip(),
                "batch_number": data.batch_number.strip(),
                "expiry_date": data.expiry_date,
                "mrp": data.mrp
            },
        )
        existing = result.scalar_one_or_none()
        if existing:
            logger.exception(f"Product with same name, batch, expiry, and MRP already exists in invoice_products for invoice: {invoice_id}")
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "error",
                    "message": f"Product with same name, batch, expiry, and MRP already exists in invoice_products: {invoice_id}",
                },
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside invoice_product_check: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
  
async def add_invoice_product(db,invoice_id,data):
    try:
        await invoice_product_check(db,invoice_id,data)
    
        product_rows= [{"id": str(uuid.uuid4()),
            "invoice_id": invoice_id,
            "product_name": data.product_name.strip(),
            "batch_number": data.batch_number.strip(),
            "expiry_date": data.expiry_date,
            "mrp": round(float(data.mrp or 0), 2),
            "actual_qty": data.actual_qty,
            "scanned_qty": data.scanned_qty,
            "scan_status": data.scan_status.value if data.scan_status else None  
        }]
        product_rows_add = await invoices_products_assign_rack_no(db,product_rows)
        final_product = product_rows_add[0]
        insert_query = """
            INSERT INTO invoice_product_list 
            (id, invoice_id, product_name, batch_number, expiry_date, mrp, actual_qty, scanned_qty, rack_no, scan_status)
            VALUES (:id, :invoice_id, :product_name, :batch_number, :expiry_date, :mrp, :actual_qty, :scanned_qty, :rack_no, :scan_status)
        """
        
        await db.execute(text(insert_query), product_rows_add)
        await db.commit()
        return final_product
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside add_invoice_product function: {e}")
        raise HTTPException(
            status_code=400,
            detail={"status": "error", "message": str(e).split("\n")[0][:100]},
        )
        
def detect_operation_status(data: InvoiceMetadataUpdateSchema) -> str | None:
    if data.picker_end and data.picker_end > 0:
        return "picker_end"
    if data.checker_end and data.checker_end > 0:
        return "checker_end"
    if data.packer_end and data.packer_end > 0:
        return "packer_end"
    return None



async def preparing_fields_invoice_metadata(data,existing_row,input_fields,invoice_id,current_user):
    try:
        update_fields = {}
        user_id = current_user.id
        status_value = getattr(data, "status", None)  # save separately
        if status_value in [
            InvoiceStatus.checking_end,
            InvoiceStatus.picking_end,
        ]:
            operation_status = detect_operation_status(data)
            if not operation_status:
                logger.warning(f"No valid *_end field detected for invoice: {invoice_id}")
                raise HTTPException(
                    status_code=400,
                    detail={
                        "status": "error",
                        "message": f"Invalid request: END status provided but no valid *_end field (picker_end/checker_end/packer_end > 0) found for invoice {invoice_id}",
                        "data": []
                    }
                )
        for field,value in input_fields.items():
            if field == "status":
                # status_value = data.status  
                continue
            if value :
                if existing_row and field.endswith("_start") and existing_row.get(field):
                    logger.info(f"Skipping update for {field}: already set in DB")
                    continue
                formatted_time = epoch_to_str(value)
                update_fields[field] = formatted_time
                update_fields[invoices_metadata_field_map[field]] = user_id
        return update_fields,status_value
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside preparing_fields_invoice_metadata function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        
async def insert_into_invoice_metadata(db,existing_row,update_fields,invoice_id):
    try:
        if existing_row:
            set_clause = ", ".join([f"{key} = :{key}" for key in update_fields.keys()])
            base_query = f"""
                    update invoice_metadata set {set_clause} where invoice_id = :invoice_id
                """
            await db.execute(text(base_query),{**update_fields,"invoice_id":invoice_id})
        else:
            new_id = str(uuid.uuid4())
            inserts_fields = {"id":new_id, "invoice_id":invoice_id, **update_fields}
            columns = ", ".join(inserts_fields.keys())
            values = ", ".join([f":{key}" for key in inserts_fields.keys()])
            base_query= f"""
                insert into invoice_metadata ({columns}) values ({values})
                """
            await db.execute(text(base_query), inserts_fields)
    except Exception as e:
        logger.exception(f"Inside insert_into_invoice_metadata function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        
async def add_transactions(data,db,current_user):
    try:
        insert_query = text(""" 
            INSERT INTO transactions (id, timestamp, invoice_id, user_id, rack_id, operation_type, 
                operation_status, scan_status, image, invoice_product_id)
            VALUES (:id, :timestamp, :invoice_id, :user_id, :rack_id, :operation_type, :operation_status,
            :scan_status, :image, :invoice_product_id)
        """)

        bulk_params = []

        for item in data.products:

            bulk_params.append({
                "id": str(uuid.uuid4()),
                "timestamp": epoch_to_str(item.timestamp),
                "invoice_id": data.invoice_id,
                "user_id": current_user.id,
                "rack_id": data.rack_id,
                "operation_type": item.operation_type.value,
                "operation_status": item.operation_status.value,
                "scan_status": item.scan_status.value,
                "image": item.image,
                "invoice_product_id": item.invoice_product_id or None,
            })
        await db.execute(insert_query, bulk_params)
        await db.commit()
        logger.info("Transaction inserted successfully")
        
    except Exception as e:
        logger.exception(f"Inside add_transactions function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        
# async def validate_tray_master_csv(db: AsyncSession, rows: list[dict]):
async def check_tray_no_tray_master(db: AsyncSession, rows: list[dict]):
    try:
        # tray_nos = []
        # duplicate_csv = []

        # ---------------- CSV Validations ----------------
        for index, row in enumerate(rows, start=1):
            tray_no = (row.get("tray_no") or "").strip()

            # tray_no must exist
            if not tray_no:
                logger.exception(f"Row {index}: tray_no is required")
                raise HTTPException(
                    status_code=400,
                    detail={"status":"error","message":f"Row {index}: tray_no is required"}
                )

            # Duplicate inside CSV
        #     if tray_no in tray_nos:
        #         duplicate_csv.append(tray_no)
        #     tray_nos.append(tray_no)

        # if duplicate_csv:
        #     logger.exception(f"Duplicate tray_no found in CSV: {list(set(duplicate_csv))}")
        #     raise HTTPException(
        #         status_code=400,
        #         detail={"status":"error", "message":f"Duplicate tray_no found in CSV: {list(set(duplicate_csv))}"}
        #     )

        # # ---------------- DB Check Using SQL ----------------
        # placeholders = ", ".join([f":t{i}" for i in range(len(tray_nos))])
        # sql = f"""
        #     SELECT tray_no FROM tray_master
        #     WHERE tray_no IN ({placeholders})
        # """
        # params = {f"t{i}": tray_nos[i] for i in range(len(tray_nos))}
        # result = await db.execute(text(sql), params)
        # existing = [row[0] for row in result.fetchall()]

        # if existing:
        #     logger.exception(f"tray_no already exists in database: {existing}")
        #     raise HTTPException(
        #         status_code=400,
        #         detail={"status":"error","message":f"tray_no already exists in database: {existing}"}
        #     )

        # return True
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Inside check_tray_no_tray_master: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        
async def prepare_tray_master_data(rows: list[dict]):
    try:
        prepared = []

        for row in rows:
            tray_no = row["tray_no"].strip()
            tray_qr_value = (row.get("tray_qr_value") or "").strip()

            prepared.append({
                "tray_no": tray_no,
                "tray_qr_value": tray_qr_value,
            })

        return prepared
    except Exception as e:
        logger.exception(f"Inside prepare_tray_master_data: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        
async def save_tray_master_data(db: AsyncSession, tray_data: list[dict]):
    try:
        if not tray_data:
            logger.info("No tray data to insert")
            return {"message": "No tray data to insert"}

        # Bulk insert using executemany
        await db.execute(text("""
            INSERT INTO tray_master (tray_no, tray_qr_value)
            VALUES (:tray_no, :tray_qr_value)
            ON CONFLICT (tray_no) DO UPDATE SET
                tray_qr_value = EXCLUDED.tray_qr_value;
        """), tray_data)
        await db.commit()

        await db.commit()
        
        return {
            "message": f"{len(tray_data)} tray records inserted successfully"
        }
    except Exception as e:
        logger.exception(f"Inside prepare_tray_master_data: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        

async def search_batch_number_invoice(db, invoice_id, batch_number, page,page_size ):
    try:
        pattern = f"{batch_number}%"
        fallback_count_query = """
            SELECT COUNT(*)
            FROM invoice_product_list
            WHERE invoice_id = :invoice_id
              AND LOWER(batch_number) LIKE LOWER(:pattern)
        """

        fallback_count_res = await db.execute(
            text(fallback_count_query),
            {"invoice_id": invoice_id, "pattern": pattern}
        )
        fallback_total = fallback_count_res.scalar() or 0

        if fallback_total == 0:
            logger.info("No matching batch numbers found in invoice_product_list")
            return {
                "status": "success",
                "message": "No matching batch numbers found in invoice_product_list",
                "page": page,
                "page_size": page_size,
                "total": 0,
                "data": []
            }
            
        
        offset = (page - 1) * page_size
        fallback_query = """
            SELECT *
            FROM invoice_product_list
            WHERE invoice_id = :invoice_id
              AND LOWER(batch_number) LIKE LOWER(:pattern)
            ORDER BY 
                substr(expiry_date, 7, 4) || '-' || substr(expiry_date, 4, 2) || '-' || substr(expiry_date, 1, 2)
            ASC
            LIMIT :limit OFFSET :offset
        """

        fallback_result = await db.execute(
            text(fallback_query),
            {
                "invoice_id": invoice_id,
                "pattern": pattern,
                "limit": page_size,
                "offset": offset
            }
        )

        fallback_rows = fallback_result.mappings().all()
        logger.info(f"Found {len(fallback_rows)} products for batch pattern '{batch_number}' in InvoiceProductList")
        return {
            "status": "success",
            "message": "Found products in invoice_product_list",
            "page": page,
            "page_size": page_size,
            "total": fallback_total,
            "data": fallback_rows
        }
    except Exception as e:
        logger.exception(f"Inside : search_batch_number_invoice {e}")
        raise HTTPException(status_code=500, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100], "data":[]})
        
        

def get_user_productivity_report(): 
    query = """
        WITH converted AS (
            SELECT 
                *,
                substr(timestamp,7,4) || '-' || substr(timestamp,4,2) || '-' || substr(timestamp,1,2) || substr(timestamp,11) AS ts_conv
            FROM transactions
        ),
        invoice_times AS (
            SELECT 
                invoice_id,
                user_id,
                MIN(ts_conv) AS start_time,
                MAX(ts_conv) AS end_time
            FROM converted
            GROUP BY invoice_id, user_id
        ),
        durations AS (
            SELECT 
                user_id,
                invoice_id,
                CAST(strftime('%s',end_time) - strftime('%s',start_time) AS INTEGER) AS invoice_duration_seconds,
                (SELECT COUNT(*) FROM converted t WHERE t.invoice_id = i.invoice_id) AS entries_in_invoice
            FROM invoice_times i
            WHERE end_time IS NOT NULL  -- count only finished invoices
        )

        SELECT 
            d.user_id,
            
            strftime('%d-%m-%Y %H:%M:%S', (SELECT MIN(start_time) FROM invoice_times WHERE user_id=d.user_id)) AS user_start_time,

            strftime('%d-%m-%Y %H:%M:%S', (SELECT MAX(end_time) FROM invoice_times WHERE user_id=d.user_id)) AS user_complete_time,

            (SELECT COUNT(*) FROM converted t WHERE t.user_id=d.user_id)
                AS user_total_entries,

            SUM(d.invoice_duration_seconds) AS user_duration_seconds,  -- actual working time only

            --ROUND((SUM(d.entries_in_invoice) * 60.0) / NULLIF(SUM(d.invoice_duration_seconds),0),3)
            --    AS user_entries_per_minute
            CASE 
                WHEN SUM(d.invoice_duration_seconds) = 0 THEN SUM(d.entries_in_invoice)  
                ELSE ROUND((SUM(d.entries_in_invoice) * 60.0) / SUM(d.invoice_duration_seconds),3)
            END AS user_entries_per_minute
        
        FROM durations d
        GROUP BY d.user_id;
        """
    return query


def calculate_seconds_diff(start_str: str, end_str: str) -> int:
    """
    Converts 'DD-MM-YYYY HH:MM:SS' → seconds difference
    """
    start_dt = datetime.strptime(start_str, DATETIME_FORMAT)
    end_dt = datetime.strptime(end_str, DATETIME_FORMAT)
    return int((end_dt - start_dt).total_seconds())


def calculate_median_time_between_scans(timestamps: list[str]) -> float | None:
    """
    timestamps = list of transaction timestamp strings for ONE invoice
    """

    if len(timestamps) < 2:
        return None  # Cannot calculate gap with < 2 entries

    # Convert to datetime objects
    dt_list = [
        datetime.strptime(ts, DATETIME_FORMAT)
        for ts in timestamps
    ]

    # Sort timestamps
    dt_list.sort()

    # Calculate consecutive differences in seconds
    time_diffs = [
        int((dt_list[i] - dt_list[i - 1]).total_seconds())
        for i in range(1, len(dt_list))
    ]

    # Median of differences
    return float(statistics.median(time_diffs))


async def get_transaction_timestamps(db: AsyncSession, invoice_id: str,operation_status: str):
    result = await db.execute(
        text("""
            SELECT timestamp 
            FROM transactions
            WHERE invoice_id = :invoice_id AND operation_status = :operation_status
            ORDER BY timestamp
        """),
        {"invoice_id": invoice_id,"operation_status":operation_status}
    )

    return [row.timestamp for row in result.fetchall()]


async def get_valid_scan_statuses(db: AsyncSession, invoice_id: str, operation_status: str):
    result = await db.execute(
        text("""
            SELECT scan_status
            FROM transactions
            WHERE invoice_id = :invoice_id
              AND operation_status = :operation_status
              AND scan_status IS NOT NULL
              AND scan_status != :manual
        """),
        {
            "invoice_id": invoice_id,
            "operation_status":operation_status,
            "manual": ScanStatusEnum.manual.value
        }
    )

    return [row.scan_status for row in result.fetchall()]


async def get_invoice_metadata_obj(db,invoice_id):
    try:
        query = text("""
        SELECT 
            picker_start, picker_end,
            checker_start, checker_end,
            packer_start, packer_end,
            picker_id, checker_id, packer_id
        FROM invoice_metadata
        WHERE invoice_id = :invoice_id
        """)

        result = await db.execute(query, {"invoice_id": invoice_id})
        meta = result.mappings().first()
        return meta
    except Exception as e:
        logger.exception(f"Inside invoice_metadata function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        
        
def metadata_end_field_find(meta,invoice_id,operation_status):
    try:
        status_field_map = {
            "picker_end": ("picker_start", "picker_end", "picker_id"),
            "checker_end": ("checker_start", "checker_end", "checker_id"),
            # "packer_end": ("packer_start", "packer_end", "packer_id"),
        }
        field_set = status_field_map.get(operation_status)
        if not field_set:
            logger.warning(
                f"Invalid operation_status {operation_status} for invoice {invoice_id}"
            )
            return None, None, None
        
        start_field, end_field, operator_field = field_set
    
        invoice_start_time = meta[start_field]
        invoice_end_time = meta[end_field]
        operator_id = meta[operator_field]
        if not invoice_start_time or not invoice_end_time:
            logger.warning(
                f"Missing {start_field}/{end_field} for invoice {invoice_id}"
            )
            return None, None, None

        return invoice_start_time, invoice_end_time, operator_id

    except Exception as e:
        logger.exception(f"Inside metadata_end_field_find function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
    
async def get_invoice_product_count(db: AsyncSession, invoice_id: str) -> int:
    try:
        query = text("""
            SELECT COUNT(*) 
            FROM invoice_product_list 
            WHERE invoice_id = :invoice_id
        """)
        
        result = await db.execute(query, {"invoice_id": invoice_id})
        return result.scalar() or 0
    except Exception as e:
        logger.exception(f"Inside get_invoice_product_count function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})
        

def epoch_to_ymd_hms(epoch_val: int) -> str:
    """Convert epoch integer to 'YYYY-MM-DD HH:MM:SS' string."""
    return datetime.fromtimestamp(epoch_val).strftime("%Y-%m-%d %H:%M:%S")


def ddmmyyyy_to_ymd_hms(date_str: str) -> str:
    """
    Convert 'DD-MM-YYYY HH:MM:SS' string to 'YYYY-MM-DD HH:MM:SS' string.
    """
    if not date_str:
        return None
    # Parse the input string
    dt_obj = datetime.strptime(date_str, "%d-%m-%Y %H:%M:%S")
    # Format as YYYY-MM-DD HH:MM:SS
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S")


async def compute_performance_metrics(db,invoice_id,data,operation_status):
    try:
        meta = await get_invoice_metadata_obj(db,invoice_id)
        if not meta:
            logger.warning(f"No invoice metadata found for {invoice_id}")
            return

        # 2. Dynamically determine which END is present
        invoice_start_time, invoice_end_time, operator_id = metadata_end_field_find(meta,invoice_id,operation_status)
        if not invoice_start_time or not invoice_end_time:
            logger.warning(f"Skipping performance metrics for invoice {invoice_id} (missing start/end)")
            return
        
        invoice_products_count = await get_invoice_product_count(db,invoice_id)
        
        txn_count_query = text("""
            SELECT COUNT(*) 
            FROM transactions 
            WHERE invoice_id = :invoice_id AND operation_status = :operation_status;
        """)
        txn_result = await db.execute(txn_count_query, {"invoice_id": invoice_id,"operation_status":operation_status})
        transaction_count = txn_result.scalar() or 0
        
        time_in_seconds = calculate_seconds_diff(invoice_start_time, invoice_end_time)
        
        timestamps = await get_transaction_timestamps(db, invoice_id,operation_status)
        median_gap = calculate_median_time_between_scans(timestamps)

        scan_statuses = await get_valid_scan_statuses(db, invoice_id,operation_status)
        accuracy = (
            round((len(scan_statuses) / transaction_count) * 100, 2)
            if transaction_count > 0 else 0.0
        )
        
        invoice_start_time_str = ddmmyyyy_to_ymd_hms(invoice_start_time)
        invoice_end_time_str = ddmmyyyy_to_ymd_hms(invoice_end_time)
        
        
        # 3. Insert/update performance metrics
        insert_query = text("""
            INSERT INTO performance_metrics (
                id,
                operator_id,
                invoice_id,
                operation_status,
                invoice_start_time,
                invoice_end_time,
                line_items,
                time_to_pick,
                total_scans,
                median_time_btw_2_scans,
                accuracy,
                created_at,
                updated_at
            )
            VALUES (
                :id,
                :operator_id,
                :invoice_id,
                :operation_status,
                :invoice_start_time,
                :invoice_end_time,
                :line_items,
                :time_to_pick,
                :total_scans,
                :median_time_btw_2_scans,
                :accuracy,
                :created_at,
                :updated_at
            )
            ON CONFLICT (invoice_id, operation_status) DO UPDATE SET
                invoice_start_time = EXCLUDED.invoice_start_time,
                invoice_end_time = EXCLUDED.invoice_end_time,
                operator_id = EXCLUDED.operator_id,
                line_items = EXCLUDED.line_items,
                time_to_pick = EXCLUDED.time_to_pick,
                total_scans = EXCLUDED.total_scans,
                median_time_btw_2_scans = EXCLUDED.median_time_btw_2_scans,
                accuracy = EXCLUDED.accuracy,
                updated_at = EXCLUDED.updated_at
        """)

        await db.execute(insert_query, {
            "id": str(uuid.uuid4()),
            "operator_id": operator_id,
            "invoice_id": invoice_id,
            "operation_status":operation_status,
            "invoice_start_time": invoice_start_time_str,
            "invoice_end_time": invoice_end_time_str,
            "line_items":invoice_products_count,
            "time_to_pick":time_in_seconds,
            "total_scans":transaction_count,
            "median_time_btw_2_scans": median_gap,
            "accuracy":accuracy,
            "created_at": invoice_end_time_str,
            "updated_at":invoice_end_time_str
        })

        await db.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"compute_performance_metrics function: {e}")
        raise HTTPException(status_code=400, detail={"status" : "error",
                "message" : str(e).split("\n")[0][:100]})