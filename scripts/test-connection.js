require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

console.log('🔍 Environment check:');
console.log('SUPABASE_URL:', process.env.SUPABASE_URL ? '✅ Set' : '❌ MISSING');
console.log('SUPABASE_SERVICE_KEY:', process.env.SUPABASE_SERVICE_KEY ? '✅ Set' : '❌ MISSING');
console.log('ANTHROPIC_API_KEY:', process.env.ANTHROPIC_API_KEY ? '✅ Set' : '❌ MISSING');
console.log('VOYAGE_API_KEY:', process.env.VOYAGE_API_KEY ? '✅ Set' : '❌ MISSING');
console.log('');
console.log('URL value:', process.env.SUPABASE_URL);

async function test() {
  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
  );

  console.log('\n🔍 Testing Supabase connection...');

  const { data, error } = await supabase
    .from('companies')
    .select('*')
    .limit(3);

  if (error) {
    console.error('❌ Connection failed:');
    console.error('   Error:', error.message);
    console.error('   Details:', error.details);
    console.error('   Hint:', error.hint);
  } else {
    console.log('✅ Supabase connected!');
    console.log(`📊 Sample data: ${data.length} rows`);
    data.forEach(c => console.log(`   - ${c.Name}`));
  }
}

test();