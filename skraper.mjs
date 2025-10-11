import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { open } from 'sqlite';
import sqlite3 from 'sqlite3';
import pLimit from 'p-limit';

// --- Configuration ---
const proxyAgent = new HttpsProxyAgent(
  'http://callmore-rotate:callmore@p.webshare.io:80'
);

// --- Supabase Client Setup ---
const supabaseUrl = 'https://ffvuhqkyskbkjfplhyol.supabase.co';
const supabaseKey = 'sb_secret_ntDL2_mIhBSDgORs0KeHUQ_bgTzX0vv';
const supabase = createClient(supabaseUrl, supabaseKey);


// --- Supabase Data Insertion Functions ---
/**
 * Converts DD.MM.YYYY or YYYY-MM-DD string to a valid YYYY-MM-DD format.
 * Returns null if the input is invalid.
 * @param {string} dateString 
 * @returns {string|null}
 */
function formatDate(dateString) {
    if (!dateString || typeof dateString !== 'string') {
      return null;
    }
    // Handles DD.MM.YYYY format
    if (dateString.includes('.')) {
      const parts = dateString.split('.');
      if (parts.length === 3) {
        return `${parts[2]}-${parts[1]}-${parts[0]}`;
      }
    }
    // Assumes YYYY-MM-DD format is already correct
    if (dateString.includes('-')) {
      return dateString;
    }
    return null; // Return null for any other invalid format
  }

  
  /**
   * Inserts comprehensive data for a single company into Supabase.
   * @param {import('@supabase/supabase-js').SupabaseClient} supabase
   * @param {object} companyData - The main company object from the scraper.
   * @param {string} companyUrl - The URL the company was scraped from.
   */
/**
 * A helper function for efficient batch upserts.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {string} tableName - The name of the table to upsert into.
 * @param {Array<object>} data - The array of data objects to insert/update.
 * @param {string} onConflict - The column(s) that define a conflict (must be a UNIQUE constraint).
 */
async function batchUpsert(supabase, tableName, data, onConflict) {
  if (!data || data.length === 0) {
    return; // Do nothing if there's no data
  }
  const { error } = await supabase.from(tableName).upsert(data, { onConflict });
  if (error) {
    console.error(`[DB-ERROR] Table: ${tableName} -`, error.message);
  }
}

/**
 * Inserts comprehensive data for a single company into Supabase.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {object} companyData - The main company object from the scraper.
 * @param {string} companyUrl - The URL the company was scraped from.
 */
async function insertCompanyData(supabase, companyData, companyUrl) {
    const companyId = companyData.orgnr; 
    const companyUrlId = companyUrl ? companyUrl.split('/').pop() : null;
  
    if (!companyId) {
      console.warn(`[DB] Skipping company insert for "${companyData.name}" due to missing orgnr.`);
      return;
    }
  
    try {
      let employeeCount = null;
      const employeeData = companyData.numberOfEmployees;

      if (typeof employeeData === 'number') {
        employeeCount = employeeData;
      } else if (typeof employeeData === 'string' && employeeData) {
        // Handles ranges like "1-4" by taking the first number
        const parsed = parseInt(employeeData.split('-')[0], 10);
        if (!isNaN(parsed)) {
          employeeCount = parsed;
        }
      }

      // --- NEW: Find the company auditor from the roles object ---
      let auditorInfo = { name: null, orgnr: null };
      const revisionGroup = companyData.roles?.roleGroups?.find(g => g.name === 'Revision');
      if (revisionGroup) {
          const auditorCompany = revisionGroup.roles.find(r => r.type === 'Company');
          if (auditorCompany) {
              auditorInfo.name = auditorCompany.name;
              auditorInfo.orgnr = auditorCompany.id;
          }
      }

      // 1. Upsert Main Company Info, now with all fields
      const { error: companyError } = await supabase.from('companies').upsert({
        company_id: companyId,
        url_id: companyUrlId,
        name: companyData.name,
        orgnr: companyData.orgnr,
        type: companyData.companyType?.name,
        status: companyData.status?.status,
        phone: companyData.phone,
        home_page: companyData.homePage,
        email: companyData.email,
        description: companyData.description,
        logo_url: companyData.logoUrl,
        legal_name: companyData.legalName,
        employees: employeeCount,
        purpose: companyData.purpose,
        registration_date: formatDate(companyData.registrationDate),
        foundation_date: formatDate(companyData.foundationDate),
        share_capital: companyData.shareCapital,
        revenue: companyData.revenue,
        profit: companyData.profit,
        mortgages: companyData.mortgages,
        registered_for_vat: companyData.registeredForVat,
        registered_for_payroll_tax: companyData.registeredForPayrollTax,
        phone2: companyData.phone2,
        mobile: companyData.mobile,
        fax_number: companyData.faxNumber,
        marketing_protection: companyData.marketingProtection,
        tagline: companyData.tagLine,
        number_of_vehicles: companyData.vehicles?.numberOfVehicles,
        contact_person_name: companyData.contactPerson?.name,
        contact_person_role: companyData.contactPerson?.role,
        certificates: companyData.certificates,
        mergers: companyData.mergers,
        videos: companyData.videos,
        auditor_company_name: auditorInfo.name,
        auditor_company_orgnr: auditorInfo.orgnr,

      }, { onConflict: 'company_id' });
  
      if (companyError) throw new Error(`Company Table: ${companyError.message}`);
  
      // The rest of the function for related tables remains the same
      const addresses = [];
      if (companyData.visitorAddress) addresses.push({ type: 'visitor', ...companyData.visitorAddress });
      if (companyData.postalAddress) addresses.push({ type: 'postal', ...companyData.postalAddress });
      await batchUpsert(supabase, 'addresses', addresses.map(addr => ({
        company_id: companyId,
        address_type: addr.type,
        address_line: addr.addressLine || addr.address,
        zip_code: addr.zipCode || addr.zip,
        post_place: addr.postPlace || addr.place,
      })), 'company_id, address_type, address_line');
  
      if (companyData.location) {
        await batchUpsert(supabase, 'locations', [{
          company_id: companyId, county: companyData.location.county, municipality: companyData.location.municipality,
          x_coordinate: companyData.location.coordinates?.[0]?.xcoordinate, y_coordinate: companyData.location.coordinates?.[0]?.ycoordinate
        }], 'company_id');
      }
      
      await batchUpsert(supabase, 'nace_industries', companyData.naceIndustries?.map(nace => ({ company_id: companyId, nace_code_and_description: nace })), 'company_id, nace_code_and_description');
      await batchUpsert(supabase, 'shareholders', companyData.shareholders?.map(sh => ({ company_id: companyId, name: sh.name, share: sh.share, number_of_shares: sh.numberOfShares })), 'company_id, name');
      await batchUpsert(supabase, 'signatories', companyData.signatories?.map(text => ({ company_id: companyId, signatory_text: text })), 'company_id, signatory_text');
      await batchUpsert(supabase, 'announcements', companyData.announcements?.map(a => ({ announcement_id: a.id, company_id: companyId, announcement_date: formatDate(a.date), announcement_text: a.text, announcement_type: a.type })), 'announcement_id');
      await batchUpsert(supabase, 'business_units', companyData.businessUnits?.map(u => ({ company_id: companyId, business_unit_name: u.name })), 'company_id, business_unit_name');
      await batchUpsert(supabase, 'industries', companyData.industries?.map(i => ({ company_id: companyId, industry_code: i.code, industry_name: i.name })), 'company_id, industry_code');
      
      if (companyData.corporateStructure) {
        await batchUpsert(supabase, 'corporate_structure', [{ company_id: companyId, parent_company_name: companyData.corporateStructure.parentCompanyName, parent_company_orgnr: companyData.corporateStructure.parentCompanyOrganisationNumber }], 'company_id');
      }
      if (companyData.availableReports) {
        await batchUpsert(supabase, 'available_reports', [{ company_id: companyId, bv_registration_certificate: companyData.availableReports.BV_REGISTRATION_CERTIFICATE, bv_report: companyData.availableReports.BV_REPORT, group_valuation: companyData.availableReports.GROUP_VALUATION, company_valuation: companyData.availableReports.COMPANY_VALUATION }], 'company_id');
      }
  
      if (companyData.companyAccounts && companyData.companyAccounts.length > 0) {
        for (const account of companyData.companyAccounts) {
          const { data: report, error: reportError } = await supabase
            .from('annual_reports')
            .upsert({
              company_id: companyId,
              year: account.year,
              period: account.period,
              length_months: account.length,
              currency: account.currency,
              period_start: formatDate(account.periodStart),
              period_end: formatDate(account.periodEnd),
              is_combined: account.combined,
              remark: account.remark
            }, { onConflict: 'company_id, year' })
            .select('report_id')
            .single();
  
          if (reportError) {
            console.error(`[DB-ERROR] Annual Report for year ${account.year}:`, reportError.message);
            continue;
          }
  
          if (report && account.accounts?.length > 0) {
            const accountsData = account.accounts
              .filter(detail => detail.amount != null)
              .map(detail => ({
                report_id: report.report_id,
                account_code: detail.code,
                amount: detail.amount,
              }));
            await batchUpsert(supabase, 'report_accounts', accountsData, 'report_id, account_code');
          }
        }
      }

      console.log(`    [DB] Successfully saved data for "${companyData.name}"`);
    } catch (error) {
      console.error(`[DB] Error saving company ${companyId}:`, error.message);
    }
}
/**
 * Inserts comprehensive data for a single person into Supabase.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase
 * @param {object} personData
 */
async function insertPersonData(supabase, personData) {
    const personId = personData.personId;
    if (!personId) {
      console.warn('[DB] Skipping person with no ID.');
      return;
    }
  
    try {
      // 1. Upsert Person's main details
      const { error: personError } = await supabase.from('people').upsert({
        person_id: personId,
        url_id: personData.urlId,
        name: personData.name,
        age: personData.age,
        year_of_birth: personData.yearOfBirth,
        birth_date: personData.birthDate,
        gender: personData.gender,
        is_business_person: personData.businessPerson,
        number_of_roles: personData.numberOfRoles,
      }, { onConflict: 'person_id' });
      if (personError) throw personError;

      const validRoles = personData.roles?.filter(role => role.id) || [];

      if (validRoles.length > 0) {
          // --- FIX: Clean the 'employees' data before sending to the database ---
          const companiesFromRoles = validRoles.map(role => {
            let employeeCount = null;
            const employeeData = role.companyNumberOfEmployees;

            if (typeof employeeData === 'number') {
              employeeCount = employeeData;
            } else if (typeof employeeData === 'string' && employeeData) {
              // Handles ranges like "1-4" by taking the first number
              const parsed = parseInt(employeeData.split('-')[0], 10);
              if (!isNaN(parsed)) {
                employeeCount = parsed;
              }
            }
            
            return {
                company_id: role.id,
                name: role.name,
                type: role.type,
                status: role.status?.status,
                employees: employeeCount // Use the cleaned, safe value
            };
          });
          
          await supabase.from('companies').upsert(companiesFromRoles, { onConflict: 'company_id', ignoreDuplicates: true });
    
          const rolesData = validRoles.map(role => ({
            person_id: personId,
            company_id: role.id,
            role_title: role.role,
          }));
          const { error } = await supabase.from('person_roles').upsert(rolesData, { onConflict: 'person_id, company_id, role_title' });
          if (error) console.error('[DB] Person Roles Error:', error.message);
      }
      
      if (personData.connections?.length > 0) {
        const connectionsData = personData.connections.map(conn => ({
          source_person_id: personId,
          connected_person_id: conn.personId,
          connected_person_name: conn.name,
          gender: conn.gender,
          is_business_connection: conn.businessConnection,
        }));
        const { error } = await supabase.from('person_connections').upsert(connectionsData, { onConflict: 'source_person_id, connected_person_id' });
        if (error) console.error('[DB] Person Connections Error:', error.message);
      }
  
      if (personData.industryConnections?.length > 0) {
          const industriesMasterData = personData.industryConnections.map(ind => ({
              industry_code: ind.industryCode,
              name: ind.name
          }));
          await supabase.from('industries_master').upsert(industriesMasterData, { onConflict: 'industry_code', ignoreDuplicates: true });
  
          const personIndustriesData = personData.industryConnections.map(ind => ({
              person_id: personId,
              industry_code: ind.industryCode
          }));
          const { error } = await supabase.from('person_industries').upsert(personIndustriesData, { onConflict: 'person_id, industry_code' });
          if(error) console.error('[DB] Person Industries Error:', error.message);
      }
  
      console.log(`      [DB] Successfully saved data for person "${personData.name}"`);
    } catch (error) {
      console.error(`[DB] Error saving person ${personId}:`, error.message);
    }
}


// --- Scraper Functions ---

async function fetchWithRetry(url, options, retries = 5, retryDelay = 1000) {
    for (let i = 0; i <= retries; i++) {
      try {
        const response = await fetch(url, options);
        if (response.ok) return response;
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

async function findBestCompanyUrl(searchTerm) {
    const url = `https://www.allabolag.se/bransch-sök?q=${encodeURIComponent(searchTerm)}`;
    console.log(`[SEARCH] For: "${searchTerm}"`);
    try {
      const response = await fetchWithRetry(url, { headers: { 'User-Agent': 'Mozilla/5.0' }, agent: proxyAgent });
      const html = await response.text();
      const $ = cheerio.load(html);
      let bestLink = null;
      let highestSimilarity = 0;
      const searchTokens = new Set(searchTerm.toLowerCase().split(/\s+/));
      $('a.addax-cs_hl_hit_company_name_click').each((_, el) => {
        const href = $(el).attr('href');
        const linkText = $(el).text().trim().toLowerCase();
        if (!href || !linkText) return;
        const linkTokens = new Set(linkText.split(/\s+/));
        const intersectionSize = [...searchTokens].filter(token => linkTokens.has(token)).length;
        const similarity = intersectionSize / searchTokens.size;
        if (similarity > highestSimilarity) {
          highestSimilarity = similarity;
          bestLink = `https://www.allabolag.se${href}`;
        }
      });
      if (bestLink && highestSimilarity > 0.5) {
        console.log(`  [FOUND] Best match: ${bestLink}`);
        return bestLink;
      }
      console.warn(`  [WARN] No good match for "${searchTerm}".`);
      return null;
    } catch (error) {
      console.error(`[ERROR] Searching for "${searchTerm}":`, error.message);
      return null;
    }
}

function parsePositionRows($) {
    const rows = [];
    $('tbody.MuiTableBody-root tr').each((_, tr) => {
      const tds = $(tr).find('td');
      if (tds.length < 2) return; // Skips if not a valid row
      
      const position = $(tds[0]).text().trim();
      const $nameLink = $(tds[1]).find('a');
      
      if ($nameLink.length) {
        const href = $nameLink.attr('href') || '';
  
        // --- Filter out links to companies (like auditors) ---
        if (href.includes('/foretag/')) {
          console.log(`    [SKIP] Ignoring company auditor: "${$nameLink.text().trim()}"`);
          return; // This is a company, so we skip to the next item in the loop.
        }
  
        const profileName = $nameLink.text().trim();
        let fullUrl = href;
        if (fullUrl && !fullUrl.startsWith('http')) {
          fullUrl = `https://www.allabolag.se${fullUrl}`;
        }
  
        if (position && profileName && fullUrl) {
          rows.push({ position, profileName, profileUrl: fullUrl });
        }
      }
    });
    return rows;
  }

async function scrapeExecutiveProfile(profileUrl) {
    if (!profileUrl) return null;
    try {
      const response = await fetchWithRetry(profileUrl, { headers: { 'User-Agent': 'Mozilla/5.0' }, agent: proxyAgent });
      const html = await response.text();
      const $ = cheerio.load(html);
      const scriptTag = $('#__NEXT_DATA__');
      if (scriptTag.length === 0) throw new Error('No __NEXT_DATA__');

      const jsonData = JSON.parse(scriptTag.html());
      const personData = jsonData?.props?.pageProps?.rolePerson || null;

      // --- NEW: Attach the URL ID to the person data object ---
      if (personData) {
        const urlParts = profileUrl.split('/');
        personData.urlId = urlParts[urlParts.length - 1];
      }

      return personData;
    } catch (error) {
      console.error(`[ERROR] Scraping profile ${profileUrl}:`, error.message);
      return null;
    }
}

async function extractDataFromUrl(companyUrl) {
    try {
      const response = await fetchWithRetry(companyUrl, { headers: { 'User-Agent': 'Mozilla/5.0' }, agent: proxyAgent });
      const html = await response.text();
      const $ = cheerio.load(html);
      const scriptTag = $('#__NEXT_DATA__');
      if (scriptTag.length === 0) throw new Error('No __NEXT_DATA__');
      const jsonData = JSON.parse(scriptTag.html());
      const companyData = jsonData?.props?.pageProps?.company;
      if (!companyData) throw new Error('No company data in JSON');
  
      let allExecutivesData = [];
      const executivesLinkEl = $('a[href^="/befattningshavare/"]');
  
      // --- MODIFIED: Efficiently find and scrape only new executives ---
      if (executivesLinkEl.length > 0) {
        const executivesUrl = `https://www.allabolag.se${executivesLinkEl.attr('href')}`;
        try {
          const execResponse = await fetchWithRetry(executivesUrl, { agent: proxyAgent });
          const execHtml = await execResponse.text();
          const $exec = cheerio.load(execHtml);
          const allPeopleOnPage = parsePositionRows($exec);
  
          // 1. In-memory deduplication for people with multiple roles
          const uniquePeopleMap = new Map();
          allPeopleOnPage.forEach(person => {
            uniquePeopleMap.set(person.profileUrl, person);
          });
          const uniquePeople = Array.from(uniquePeopleMap.values());
  
          if (uniquePeople.length > 0) {
            // 2. Batch check DB for people we already have
            const personUrlIds = uniquePeople.map(p => p.profileUrl.split('/').pop());
            const { data: existingPeople, error: dbError } = await supabase
              .from('people')
              .select('url_id')
              .in('url_id', personUrlIds);
  
            if (dbError) {
              console.error(`  [DB-ERROR] Failed to check for existing people:`, dbError.message);
            }
  
            const existingIds = new Set(existingPeople ? existingPeople.map(p => p.url_id) : []);
  
            // 3. Filter to get only the people we actually need to scrape
            const peopleToScrape = uniquePeople.filter(p => {
              const urlId = p.profileUrl.split('/').pop();
              return !existingIds.has(urlId);
            });
  
            if (existingIds.size > 0) {
              console.log(`    [SKIP] Found ${uniquePeople.length} unique executives, skipping ${existingIds.size} already in DB.`);
            }
  
            // 4. Scrape only the new profiles
            if (peopleToScrape.length > 0) {
              const executivePromises = peopleToScrape.map(person => scrapeExecutiveProfile(person.profileUrl));
              allExecutivesData = (await Promise.all(executivePromises)).filter(Boolean);
            }
          }
        } catch (execError) {
          console.error(`  [ERROR] Scraping executives for ${companyData.name}:`, execError.message);
        }
      }
      return { companyData, allExecutivesData };
    } catch (error) {
      console.error(`  [ERROR] Processing ${companyUrl}:`, error.message);
      return null;
    }
  }

// --- Main Orchestrator ---
async function main() {
  console.log('--- Starting Allabolag Scraper (Simple Mode) ---');
  
  const heartbeatUrl = 'https://uptime.betterstack.com/api/v1/heartbeat/aqrzzpHiHPaqFd9F61JU17Ms';
  let heartbeatInterval = null;

  const limit = pLimit(30);
  const BATCH_SIZE = 1000;
  let totalProcessedInRun = 0;

  try {
    heartbeatInterval = setInterval(() => {
      fetch(heartbeatUrl).catch(err => console.warn('[MONITORING] Heartbeat ping failed:', err.message));
    }, 60 * 1000);

    while (true) {
      console.log(`--- Fetching a new batch of up to ${BATCH_SIZE} companies to process... ---`);
      
      const { data: batch, error: batchError } = await supabase
        .from('companies_to_scrape')
        .select('id, "companyName"')
        .not('status', 'in', '("finished", "failed")') // Fetches only companies not yet marked
        .order('id', { ascending: true })
        .limit(BATCH_SIZE);

      if (batchError) {
        console.error(`[DB-ERROR] Error fetching batch: ${batchError.message}`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      if (batch.length === 0) {
        console.log("✅ No more companies to scrape. Exiting loop.");
        break;
      }

      console.log(`[INFO] Found ${batch.length} companies in this batch to process.`);

      const tasks = batch.map(company => {
          return limit(async () => {
              const url = await findBestCompanyUrl(company.companyName);
              if (!url) {
                await supabase.from('companies_to_scrape').update({ status: 'failed' }).eq('id', company.id);
                return;
              }

              try {
                  const extractedData = await extractDataFromUrl(url);
                  if (!extractedData) {
                      await supabase.from('companies_to_scrape').update({ status: 'failed' }).eq('id', company.id);
                      return;
                  }
  
                  await insertCompanyData(supabase, extractedData.companyData, url);
  
                  if (extractedData.allExecutivesData) {
                    for (const personData of extractedData.allExecutivesData) {
                      await insertPersonData(supabase, personData);
                    }
                  }
  
                  await supabase.from('companies_to_scrape').update({ status: 'finished' }).eq('id', company.id);
                  console.log(`    [STATUS] Marked company "${company.companyName}" (ID: ${company.id}) as 'finished'.`);

              } catch (error) {
                  console.error(`[CRITICAL] An error occurred while processing company ${company.id}:`, error);
                  await supabase.from('companies_to_scrape').update({ status: 'failed' }).eq('id', company.id);
              }

              totalProcessedInRun++;
              console.log(`[PROGRESS] ${totalProcessedInRun} companies processed in this run.`);
          });
      });

      await Promise.all(tasks);
    }
  } catch (error) {
    console.error('[CRITICAL] A fatal error occurred:', error);
  } finally {
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval);
    }
    console.log('--- Scraper finished its run. ---');
  }
}

main();