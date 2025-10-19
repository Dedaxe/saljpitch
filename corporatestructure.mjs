import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';
import { HttpsProxyAgent } from 'https-proxy-agent';
import pLimit from 'p-limit';

// --- Configuration ---
const proxyAgent = new HttpsProxyAgent(
  'http://callmore-rotate:callmore@p.webshare.io:80'
);

// --- Supabase Client Setup ---
const supabaseUrl = 'https://ffvuhqkyskbkjfplhyol.supabase.co';
const supabaseKey = 'sb_secret_ntDL2_mIhBSDgORs0KeHUQ_bgTzX0vv';
const supabase = createClient(supabaseUrl, supabaseKey);

// --- Helper Functions ---

/**
 * A helper function for efficient batch upserts.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} tableName - The name of the table to upsert into.
 * @param {Array<object>} data - The array of data objects to insert/update.
 * @param {string} onConflict - The column(s) that define a conflict (must be a UNIQUE constraint).
 */
async function batchUpsert(supabase, tableName, data, onConflict) {
  if (!data || data.length === 0) {
    return;
  }
  const { error } = await supabase.from(tableName).upsert(data, { onConflict });
  if (error) {
    console.error(`[DB-ERROR] Table: ${tableName} -`, error.message);
  }
}

async function fetchWithRetry(url, options, retries = 5, retryDelay = 1000) {
  for (let i = 0; i <= retries; i++) {
    try {
      const response = await fetch(url, options);
      
      // If the request was successful, return immediately.
      if (response.ok) return response;

      // --- NEW CODE BLOCK ---
      // If the server says "Not Found", don't retry. Treat it as a final answer.
      if (response.status === 404) {
          console.log(`    [INFO] Received 404 for ${url}. No data available, not retrying.`);
          return response; // Return the 404 response to be handled by the caller
      }
      // --- END OF NEW CODE BLOCK ---

      console.warn(`[RETRY] Attempt ${i + 1}/${retries + 1} for ${url} failed with status: ${response.status}`);
    } catch (error) {
      console.warn(`[RETRY] Attempt ${i + 1}/${retries + 1} for ${url} failed with network error: ${error.message}`);
    }
    if (i === retries) break;
    const delay = retryDelay * Math.pow(2, i);
    await new Promise(resolve => setTimeout(resolve, delay));
  }
  throw new Error(`All ${retries + 1} attempts failed for ${url}`);
}

// --- Corporate Structure Scraper Functions ---

/**
 * Recursively scrapes and flattens the corporate structure tree.
 * @param {object} node - The current node in the corporate structure tree.
 * @param {Array<object>} results - The array to store the flattened results.
 * @param {string} mainCompanyOrgnr - The orgnr of the company being scraped.
 */
function flattenCorporateTree(node, results, mainCompanyOrgnr) {
    if (!node || !node.name) return;
  
    // Push only the specific fields for the corporate_structure table
    results.push({
      company_id: mainCompanyOrgnr,
      subsidiary_orgnr: node.organisationNumber || 'N/A',
      subsidiary_name: node.name,
      owned_percentage: node.ownedPercentage,
      country_code: node.countryCode,
      is_inactive: node.inactive,
    });
  
    // Recursively process any sub-nodes
    if (node.sub && Array.isArray(node.sub)) {
      for (const subNode of node.sub) {
        flattenCorporateTree(subNode, results, mainCompanyOrgnr);
      }
    }
  }

/**
 * Fetches and processes a company's corporate structure from the API.
 * @param {string} orgnr - The company's organization number.
 * @returns {Array<object>|null} The flattened corporate structure or null on failure.
 */
async function scrapeCorporateStructure(orgnr) {
  if (!orgnr) {
    return null;
  }
  const apiUrl = `https://www.allabolag.se/api/company/legal/${orgnr}/corporateStructure`;
  console.log(`  [API] Fetching corporate structure for ${orgnr}`);

  try {
    const response = await fetchWithRetry(apiUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0' },
      agent: proxyAgent
    });

    const data = await response.json();
    if (!data || !data.tree) {
      console.log(`    [INFO] No corporate structure found for ${orgnr}.`);
      return [];
    }

    const flattenedStructure = [];
    flattenCorporateTree(data.tree, flattenedStructure, orgnr);
    return flattenedStructure;
  } catch (error) {
    console.error(`  [ERROR] Scraping corporate structure for ${orgnr}:`, error.message);
    return null;
  }
}

// --- Main Orchestrator ---

// --- Main Orchestrator ---

// --- Main Orchestrator (Corrected Version) ---

async function main() {
  console.log('--- Starting Corporate Structure Scraper ---');

  const limit = pLimit(30);
  const BATCH_SIZE = 500; // A smaller batch size is often more stable
  let totalProcessedInRun = 0;

  try {
    // Keep looping as long as there are companies to process
    while (true) {
      console.log(`--- Fetching a new batch of up to ${BATCH_SIZE} companies... ---`);

      // 1. Fetch the NEXT batch of unprocessed companies. No offset needed.
      const { data: companies, error: batchError } = await supabase
        .from('companies')
        .select('orgnr')
        .is('structure_scraped_at', null) // Always get unprocessed ones
        .limit(BATCH_SIZE); // Just get the next N companies

      if (batchError) {
        console.error(`[DB-ERROR] Error fetching batch: ${batchError.message}`);
        await new Promise(resolve => setTimeout(resolve, 5000)); // Wait before retrying
        continue;
      }
      
      // 2. If the batch is empty, we are finished.
      if (!companies || companies.length === 0) {
        console.log('--- No more companies to process. ---');
        break; // Exit the while loop
      }

      console.log(`Processing ${companies.length} companies in this batch.`);
      const orgnrs = companies.map(c => c.orgnr);

      // 3. Process this smaller, manageable batch.
      const tasks = orgnrs.map(orgnr => {
        return limit(async () => {
          const corporateStructure = await scrapeCorporateStructure(orgnr);

          if (corporateStructure && corporateStructure.length > 0) {
            await batchUpsert(supabase, 'corporate_structure_2', corporateStructure, 'company_id, subsidiary_orgnr, subsidiary_name');
          }

          // Mark this single company as processed
          await supabase
            .from('companies')
            .update({ structure_scraped_at: new Date() })
            .eq('orgnr', orgnr);

          totalProcessedInRun++;
          console.log(`[PROGRESS] Processed ${totalProcessedInRun} companies in this run.`);
        });
      });

      // Wait for only THIS batch to complete before fetching the next one
      await Promise.all(tasks);
    }

  } catch (error) {
    console.error('[CRITICAL] A fatal error occurred:', error);
  } finally {
    console.log('--- Corporate Structure Scraper finished. ---');
  }
}



main();