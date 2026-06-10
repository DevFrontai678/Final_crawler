require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { detectATS } = require('../src/ats-adapters/ats-detector');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

async function run() {
  const { data: companies, error } = await supabase
    .from('companies')
    .select('*')
    .not('Website', 'is', null)
    .limit(200);

  if (error) {
    console.error('❌ Supabase error:', error.message);
    return;
  }

  console.log(`\n🔍 ATS Detection starting for ${companies.length} companies...\n`);

  const results = {
    greenhouse: 0, personio: 0, workday: 0,
    lever: 0, sap: 0, teamtailor: 0,
    softgarden: 0, rexx: 0,
    unknown: 0, error: 0, custom: 0
  };

  for (const company of companies) {
    console.log(`\n📍 ${company.Name}`);
    console.log(`   URL: ${company.Website}`);

    const result = await detectATS(company.Id, company.Website);

    console.log(`   ✅ ATS: ${result.ats_type} (confidence: ${result.ats_confidence})`);
    console.log(`   Method: ${result.detection_method}`);

    // ← KEY FIX: Low confidence = unknown
    // Claude fallback (0.6) pe trust nahi karte
    // Sirf URL pattern / HTML signature pe trust karte hain (0.75+)
    // NAYA
const atsTypeToSave = 
  result.ats_confidence >= 0.75 
    ? result.ats_type                    // High confidence → as is
    : result.ats_type === 'error'
      ? 'error'                          // Errors save karo
      : result.ats_type === 'custom' && result.ats_confidence >= 0.6
        ? 'custom'                       // Claude ne custom kaha → trust karo
        : result.ats_type === 'unknown' && result.ats_confidence >= 0.6
          ? 'custom'                     // Claude ne unknown kaha → probably custom
          : 'unknown';                   // Genuine unknown

    const updateResult = await supabase
      .from('companies')
      .update({
        ats_type: atsTypeToSave,
        ats_confidence: result.ats_confidence,
        detected_career_url: result.career_page_url,
        crawl_status: result.ats_type === 'error' ? 'failed' : 'ats_detected',
        updated_at: new Date().toISOString()
      })
      .eq('Id', company.Id);

    if (updateResult.error) {
      console.log(`    ❌ Save error: ${updateResult.error.message}`);
    } else {
      console.log(`    💾 Saved as: ${atsTypeToSave}`);
    }

    results[atsTypeToSave] = (results[atsTypeToSave] || 0) + 1;

    await new Promise(r => setTimeout(r, 1500));
  }

  console.log('\n\n📊 ===== ATS DETECTION SUMMARY =====');
  Object.entries(results)
    .filter(([_, count]) => count > 0)
    .sort((a, b) => b[1] - a[1])
    .forEach(([ats, count]) => {
      console.log(`  ${ats.padEnd(15)}: ${count} companies`);
    });
  console.log('====================================\n');
}

run();