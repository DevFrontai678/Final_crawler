// scripts/seed-candidates.js
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_MODEL = 'voyage-3';

// ── N8N Webhook Configuration ──────────────────────────────────
const N8N_WEBHOOK_URL = 'http://13.140.159.81:5678/webhook/match-by-id'; 

// ── Candidate Data (parsed from PDFs) ──────────────────────────
const candidates = [
  {
    salesforce_id: "MCG-171885",
    name: "MCG-171885 Senior IT-Administrator & IT-Projektmanager",
    location: "Kreis Heinsberg",
    seniority_level: "senior",
    remote_preference: "hybrid",
    skills: {
      "Windows Server": 4, "Windows Client": 4, "MacOS": 3, "Active Directory": 4,
      "Exchange Server": 3, "Fileserver": 3, "Webserver": 2, "Application Server": 2,
      "Terminal Server": 3, "NextCloud": 3, "Microsoft WSUS": 3, "Confluence": 3,
      "SharePoint": 3, "Synology": 3, "ERP/CRM-Systeme": 2, "Microsoft Copilot": 2,
      "Microsoft 365": 3, "Microsoft Azure": 2, "Hybrid Cloud": 2, "Microsoft Hyper-V": 3,
      "VMware Vsphere": 3, "vCenter Server": 3, "Microsoft Office": 3, "Teams/Zoom/Webex": 3,
      "SAN/NAS": 3, "Veeam": 3, "VPN/WLAN/VLAN": 3, "Routing/Switching": 3,
      "DNS/DHCP/TCP-IP": 3, "Zero Trust Networking": 2, "WatchGuard": 2, "Sophos": 2,
      "Netzwerküberwachung": 3, "EDR/XDR Security": 3, "Cloud Security": 2,
      "SQL-Datenbanken": 2, "Jira Service Desk": 2, "ITIL": 2, "Automatisierung": 2
    }
  },
  {
    salesforce_id: "MCG-195828",
    name: "MCG-195828 IT-Teamleiter IT-Manager",
    location: "Großraum Kaufbeuren / Allgäu",
    seniority_level: "senior",
    remote_preference: "onsite",
    skills: {
      "Windows Server": 4, "Windows Client": 3, "iOS": 4, "MacOS": 3,
      "Active Directory": 4, "Entra ID": 4, "Exchange Server": 3, "Fileserver": 4,
      "Webserver": 3, "Application Server": 3, "Terminal Server": 3, "SQL-Server": 3,
      "Microsoft WSUS": 3, "SharePoint": 3, "Microsoft 365": 4, "Microsoft Azure": 3,
      "GPO": 3, "Intune": 3, "Mobile Device Management": 3, "Baramundi": 2,
      "Microsoft Hyper-V": 3, "VMware Vsphere": 3, "vCenter Server": 3, "VMware ESXi": 3,
      "Microsoft Office": 3, "Microsoft Teams": 3, "NAS": 3,
      "TCP/IP/DNS/DHCP": 4, "Routing/Switching/SD-WAN": 3, "WLAN": 3, "VLAN": 3,
      "VPN": 3, "Zero Trust Network": 2, "Netzwerk-Troubleshooting": 3, "PRTG": 3,
      "Sophos": 3, "EDR/XDR Security": 3, "Cloud Security": 3, "Veeam": 3, "MsSQL": 3
    }
  },
  {
    salesforce_id: "MCG-246383",
    name: "MCG-246383 Cybersecurity Specialist",
    location: "Raum München",
    seniority_level: "senior",
    remote_preference: "hybrid",
    skills: {
      "IBM QRadar SIEM": 4, "Splunk": 4, "Suricata/Zeek IDS": 4,
      "Penetration Testing": 4, "Vulnerability Management": 4, "Incident Response": 4,
      "AWS Terraform": 3, "Microsoft Azure": 3, "Docker/GitLab CI-CD": 3,
      "Python/Bash Scripting": 3, "ISO/IEC 27001 ISMS": 4, "Tenable.io/Greenbone": 4,
      "Nmap/Wireshark/Scapy": 4, "Burp Suite/OWASP ZAP": 3, "ELK/Opensearch": 3,
      "Grafana": 3, "Microsoft Defender": 3, "Entra ID/M365": 3,
      "Linux": 3, "Nagios/Icinga/check_mk": 3, "Git/Github/Gitlab": 3
    }
  },
  {
    salesforce_id: "MCG-246644",
    name: "MCG-246644 Senior Developer Kotlin Java Spring Node.js TypeScript",
    location: "Berlin",
    seniority_level: "senior",
    remote_preference: "hybrid",
    skills: {
      "Java": 4, "Java EE/Jakarta EE": 4, "JavaServer Faces": 4, "Hibernate": 4,
      "Spring Boot": 4, "Spring": 4, "JavaScript": 4, "TypeScript": 4,
      "Node.js": 4, "HTML": 4, "SQL": 4, "MS SQL": 4, "MongoDB": 4,
      "PostgreSQL": 4, "MySQL": 4, "Docker": 4, "REST": 4, "Git": 4, "Kotlin": 4,
      "React.js": 3, "Angular": 3, "Next.js": 3, "Oracle": 3, "Linux": 3,
      "MacOS": 3, "AWS": 2, "Azure": 2, "Kubernetes": 3
    }
  },
  {
    salesforce_id: "MCG-247085",
    name: "MCG-247085 Senior IT-Administrator",
    location: "Nähe Bonn",
    seniority_level: "senior",
    remote_preference: "hybrid",
    skills: {
      "Windows Server": 4, "Windows Client": 4, "MacOS": 4, "Active Directory": 4,
      "Microsoft Azure": 3, "GPO": 4, "Intune": 3, "Baramundi": 4, "IAM": 3,
      "Microsoft Hyper-V": 3, "VMware Vsphere": 3, "vCenter Server": 3, "VMware ESXi": 3,
      "VirtualBox": 3, "Citrix": 3, "Microsoft Office": 4, "Microsoft Teams": 4,
      "TCP/IP/DNS/DHCP": 4, "WLAN": 4, "VLAN": 4, "VPN": 4, "Ethernet": 4,
      "Zero Trust Network": 4, "Netzwerk-Troubleshooting": 3, "PRTG": 3, "Zabbix": 3,
      "Checkmk": 3, "Fortinet": 3, "Sophos": 4, "EDR/XDR Security": 3,
      "Cloud Security": 3, "OTRS": 4, "Jira Service Desk": 3, "PowerShell/Bash": 3,
      "Exchange Server": 3, "Entra ID": 3, "SharePoint": 2, "Microsoft 365": 2,
      "Docker": 2, "Nagios": 2
    }
  }
];

// ── Voyage AI Embedding ─────────────────────────────────────────
async function getEmbedding(text, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.post(
        'https://api.voyageai.com/v1/embeddings',
        { model: VOYAGE_MODEL, input: [text] },
        {
          headers: {
            Authorization: `Bearer ${VOYAGE_API_KEY}`,
            'Content-Type': 'application/json',
          },
        }
      );
      return response.data.data[0].embedding;
    } catch (err) {
      if (err.response?.status === 429 && attempt < retries) {
        const wait = attempt * 5000;
        console.log(`   Rate limited. Waiting ${wait/1000}s...`);
        await new Promise(r => setTimeout(r, wait));
      } else {
        throw err;
      }
    }
  }
}

// Skills object → embedding text
function candidateToEmbeddingText(candidate) {
  const sortedSkills = Object.entries(candidate.skills)
    .sort((a, b) => b[1] - a[1])
    .map(([skill, score]) => `${skill}(${score})`)
    .join(', ');

  return `${candidate.name}. Level: ${candidate.seniority_level}. Remote: ${candidate.remote_preference}. Location: ${candidate.location}. Skills: ${sortedSkills}`;
}

// ── Trigger n8n Webhook Function ────────────────────────────────
async function triggerN8NWorkflow(salesforceId) {
  try {
    console.log(`   🚀 Triggering n8n Webhook for candidate...`);
    const response = await axios.post(N8N_WEBHOOK_URL, {
      candidate_id: salesforceId
    });
    console.log(`   🎯 n8n Response (Total Matches Found: ${response.data.total_matches || 0})`);
    if (response.data.matches && response.data.matches.length > 0) {
      console.log(`   ⭐ Top Match: ${response.data.matches[0].job_title} at ${response.data.matches[0].company} (${response.data.matches[0].score})`);
    }
  } catch (err) {
    console.error(`   ❌ n8n Webhook failed: ${err.message}`);
  }
}

// ── Main ────────────────────────────────────────────────────────
async function seedCandidates() {
  console.log(`\nSeeding ${candidates.length} candidates...\n`);

  let insertOk = 0;
  let embeddingOk = 0;

  for (const candidate of candidates) {
    console.log(`Processing: ${candidate.salesforce_id} — ${candidate.name.split(' ').slice(1, 4).join(' ')}`);

    try {
      // 1. Upsert candidate (without embedding first)
      const { data: inserted, error: insertError } = await supabase
        .from('candidates')
        .upsert({
          salesforce_id: candidate.salesforce_id,
          salesforce_contact_id: candidate.salesforce_id,  
          name: candidate.name,
          location: candidate.location,
          seniority_level: candidate.seniority_level,
          remote_preference: candidate.remote_preference,
          skill_scores: candidate.skills,   
          is_active: true,
          last_synced_at: new Date().toISOString(),
        }, { onConflict: 'salesforce_id' })
        .select('id')
        .single();

      if (insertError) throw insertError;
      insertOk++;

      // 2. Generate embedding
      const text = candidateToEmbeddingText(candidate);
      console.log(`   🔄 Generating embedding...`);

      const embedding = await getEmbedding(text);

      const { error: embError } = await supabase
        .from('candidates')
        .update({ skill_embedding: embedding })
        .eq('id', inserted.id);

      if (embError) throw embError;

      console.log(`   ✅ Embedding done (${embedding.length} dims)`);
      embeddingOk++;

      // 3. Trigger n8n Workflow (Embedding save hone ke baad run hoga)
      await triggerN8NWorkflow(candidate.salesforce_id);
      console.log('\n');

      // Rate limit buffer (Voyage AI and n8n stability)
      await new Promise(r => setTimeout(r, 8000));

    } catch (err) {
      console.error(`   ❌ Failed at this candidate step: ${err.message}\n`);
    }
  }

  console.log('─'.repeat(50));
  console.log(`Inserted  : ${insertOk}/${candidates.length}`);
  console.log(`Embeddings: ${embeddingOk}/${candidates.length}`);
  console.log('\n✅ Done! Candidates ready and workflows triggered.');
}

seedCandidates().catch(console.error);