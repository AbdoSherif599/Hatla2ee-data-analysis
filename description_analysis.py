from typing import List, Optional

from dotenv import dotenv_values
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field
from pymongo import MongoClient, UpdateOne

settings = dotenv_values(".env")


# 1. Define the Pydantic schema for the EXTRA fields only
class CarExtraDetails(BaseModel):
    color: Optional[str] = Field(
        default=None, 
        description="The exterior color of the car (e.g., Black, White, Silver) translated to English."
    )
    engine_capacity_cc: Optional[int] = Field(
        default=None, 
        description="The engine capacity strictly in CC as an integer (e.g., 2700, 3500, 4000)."
    )
    horsepower: Optional[int] = Field(
        default=None, 
        description="The engine horsepower as an integer (e.g., 277, 235, 409)."
    )
    seating_capacity: Optional[int] = Field(
        default=None, 
        description="Number of seats in the car (e.g., 5, 6, 7)."
    )
    drivetrain: Optional[str] = Field(
        default=None, 
        description="The drivetrain of the car, e.g., '4x4', 'RWD', 'FWD', 'AWD'."
    )
    trim_level: Optional[str] = Field(
        default=None, 
        description="The specific model trim or category, e.g., 'GXR', 'GR', 'Sport', 'Full Option'."
    )
    paint_condition: Optional[str] = Field(
        default=None, 
        description="The condition of the paint, e.g., 'Factory Paint (فبريكا بالكامل)', 'Zero', 'Painted Outside'."
    )
    features: List[str] = Field(
        default_factory=list, 
        description="A list of key features in English (e.g., 'Sunroof', 'Rear Camera', 'Cruise Control', 'Apple CarPlay', 'Leather Seats', 'Power Seats')."
    )
    contact_info: Optional[List[str]] = Field(
        default_factory=list, 
        description="Any contact information mentioned in the description, such as phone numbers or email addresses."
    )

# 2. Setup the output parser
parser = PydanticOutputParser(pydantic_object=CarExtraDetails)

# 3. Initialize the Gemini LLM
# Requires GEMINI_API environment variable to be set
llm = ChatGoogleGenerativeAI(
    model="gemma-4-31b-it",
    temperature=0.2, # Temperature 0 ensures the most deterministic extraction
    google_api_key=settings.get("GEMINI_API")
)

# 4. Construct the Prompt Template
prompt = PromptTemplate(
    template=(
        "You are an automotive data extraction AI.\n"
        "Extract ONLY the requested extra details from the provided car description (which is mostly in Arabic). "
        "Translate colors, drivetrain, and features into English.\n"
        "If a specific piece of information is not mentioned in the text, return null for that field.\n\n"
        "{format_instructions}\n\n"
        "Car Description:\n{description}\n"
    ),
    input_variables=["description"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

# 5. Create the abstraction chain
extraction_chain = prompt | llm | parser

# Example Extractor Function
if __name__ == "__main__":
    mongo_uri = settings.get("MONGO_URI")
    db_name = settings.get("DB_NAME")
    collection_name = settings.get("COLLECTION_NAME")
    batch_size = int(settings.get("BATCH_SIZE", 20))
    max_concurrency = int(settings.get("MAX_CONCURRENCY", 5))
    
    if not all([mongo_uri, db_name, collection_name]):
        print("Missing MongoDB environment variables (MONGO_URI, DB_NAME, COLLECTION_NAME).")
        exit(1)
        
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    documents = list(
        collection.find(
            {"description": {"$exists": True, "$ne": ""}},
            {"description": 1},
        )
    )

    if not documents:
        print("No documents found with a non-empty description.")
        raise SystemExit(0)

    print(f"Found {len(documents)} documents to process.")
    for start in range(0, len(documents), batch_size):
        chunk_docs = documents[start : start + batch_size]
        chunk_inputs = [{"description": doc["description"]} for doc in chunk_docs]
        print(
            f"Running batch {start // batch_size + 1} with {len(chunk_docs)} descriptions "
            f"(max_concurrency={max_concurrency})"
        )

        batch_results = extraction_chain.batch(
            chunk_inputs,
            config={"max_concurrency": max_concurrency},
            return_exceptions=True,
        )

        updates = []
        failed = 0
        for doc, result in zip(chunk_docs, batch_results):
            if isinstance(result, Exception):
                failed += 1
                print(f"Error processing _id {doc['_id']}: {result}")
                continue

            update_data = result.model_dump(exclude_none=True)
            if update_data:
                updates.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_data}))

        if updates:
            collection.bulk_write(updates, ordered=False)

        print(
            f"Batch done: success={len(updates)} failed={failed} "
            f"processed={start + len(chunk_docs)}/{len(documents)}"
        )