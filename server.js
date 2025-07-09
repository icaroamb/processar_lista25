const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Configuração do multer para upload de arquivos
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = './uploads';
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  }
});

const upload = multer({ 
  storage: storage,
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB max
  },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Apenas arquivos CSV são permitidos!'), false);
    }
  }
});

// Função para extrair preço numérico
function extractPrice(priceString) {
  if (!priceString || priceString.toString().trim() === '') return 0;
  
  const cleanPrice = priceString
    .toString()
    .replace(/R\$\s?/, '')
    .replace(/\./g, '')
    .replace(/,/g, '.')
    .trim();
  
  const price = parseFloat(cleanPrice);
  return isNaN(price) ? 0 : price;
}

// Função para fazer parse manual do CSV (mais robusta)
function parseCSVManual(csvContent) {
  const lines = csvContent.split('\n').filter(line => line.trim());
  if (lines.length < 2) return [];
  
  const headers = [];
  const data = [];
  
  // Processar cabeçalho
  const headerLine = lines[0];
  let currentField = '';
  let inQuotes = false;
  
  for (let i = 0; i < headerLine.length; i++) {
    const char = headerLine[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      headers.push(currentField.trim().replace(/"/g, ''));
      currentField = '';
    } else {
      currentField += char;
    }
  }
  headers.push(currentField.trim().replace(/"/g, ''));
  
  // Processar dados
  for (let lineIndex = 1; lineIndex < lines.length; lineIndex++) {
    const line = lines[lineIndex];
    const row = {};
    
    currentField = '';
    inQuotes = false;
    let fieldIndex = 0;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      
      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        row[headers[fieldIndex]] = currentField.trim().replace(/"/g, '');
        currentField = '';
        fieldIndex++;
      } else {
        currentField += char;
      }
    }
    
    // Adicionar último campo
    if (fieldIndex < headers.length) {
      row[headers[fieldIndex]] = currentField.trim().replace(/"/g, '');
    }
    
    data.push(row);
  }
  
  return data;
}

// Função para processar o CSV
function processCSV(filePath) {
  return new Promise((resolve, reject) => {
    try {
      // Ler arquivo como texto
      const fileContent = fs.readFileSync(filePath, 'utf8');
      
      // Parse manual do CSV
      const parsedData = parseCSVManual(fileContent);
      
      console.log('🔍 DEBUG: Total de linhas processadas:', parsedData.length);
      
      // Configuração das lojas e suas colunas
      const lojasConfig = [
        { nome: 'Loja da Suzy', colunas: ['Loja da Suzy', '', '_1'] },
        { nome: 'Loja Top Celulares', colunas: ['Loja Top Celulares', '_3', '_4'] },
        { nome: 'Loja HUSSEIN', colunas: ['Loja HUSSEIN', '_6', '_7'] },
        { nome: 'Loja Paulo', colunas: ['Loja Paulo', '_9', '_10'] },
        { nome: 'Loja HM', colunas: ['Loja HM', '_12', '_13'] },
        { nome: 'Loja General', colunas: ['Loja General', '_15', '_16'] },
        { nome: 'Loja JR', colunas: ['Loja JR', '_18', '_19'] },
        { nome: 'Loja Mega Cell', colunas: ['Loja Mega Cell', '_21', '_22'] }
      ];
      
      const processedData = [];
      
      // Processar cada loja
      lojasConfig.forEach((lojaConfig) => {
        console.log(`\n🔍 DEBUG: Processando ${lojaConfig.nome}...`);
        const produtos = [];
        
        // Processar todas as linhas
        for (let i = 0; i < parsedData.length; i++) {
          const row = parsedData[i];
          
          const codigo = row[lojaConfig.colunas[0]];
          const modelo = row[lojaConfig.colunas[1]];
          const preco = row[lojaConfig.colunas[2]];
          
          // Pular linha de cabeçalho
          if (codigo === 'Código' || !codigo) continue;
          
          // Adicionar produto se tiver dados válidos
          if (codigo && modelo && preco && 
              codigo.toString().trim() !== '' && 
              modelo.toString().trim() !== '' && 
              preco.toString().trim() !== '') {
            
            const precoNumerico = extractPrice(preco);
            
            if (precoNumerico > 0) {
              produtos.push({
                codigo: codigo.toString().trim(),
                modelo: modelo.toString().trim(),
                preco: precoNumerico
              });
            }
          }
        }
        
        console.log(`🔍 DEBUG: ${lojaConfig.nome} - ${produtos.length} produtos encontrados`);
        
        // Adicionar loja aos dados processados
        if (produtos.length > 0) {
          processedData.push({
            loja: lojaConfig.nome,
            total_produtos: produtos.length,
            produtos: produtos
          });
        }
      });
      
      console.log('🔍 DEBUG: Total de lojas processadas:', processedData.length);
      resolve(processedData);
      
    } catch (error) {
      console.error('❌ ERRO no processamento:', error);
      reject(error);
    }
  });
}

// Rota principal para upload e processamento
app.post('/process-csv', upload.single('csvFile'), async (req, res) => {
  try {
    console.log('📤 Recebido arquivo:', req.file ? req.file.originalname : 'Nenhum');
    
    if (!req.file) {
      return res.status(400).json({ 
        error: 'Nenhum arquivo CSV foi enviado' 
      });
    }
    
    const filePath = req.file.path;
    console.log('📁 Processando arquivo:', filePath);
    
    // Processar o CSV
    const processedData = await processCSV(filePath);
    
    // Limpar arquivo temporário
    fs.unlinkSync(filePath);
    
    console.log('✅ Processamento concluído. Dados retornados:', processedData.length, 'lojas');
    
    // Retornar dados processados
    res.json(processedData);
    
  } catch (error) {
    console.error('❌ Erro ao processar CSV:', error);
    
    // Limpar arquivo se existir
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    
    res.status(500).json({ 
      error: 'Erro interno do servidor',
      details: error.message 
    });
  }
});

// Rota para teste de saúde
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    message: 'API funcionando corretamente',
    timestamp: new Date().toISOString()
  });
});

// Rota de documentação
app.get('/', (req, res) => {
  res.json({
    message: 'API para processamento de CSV de produtos',
    version: '1.0.0',
    endpoints: {
      'POST /process-csv': 'Envia arquivo CSV e retorna JSON estruturado',
      'GET /health': 'Verifica status da API'
    }
  });
});

// Middleware de tratamento de erros
app.use((error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    if (error.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ 
        error: 'Arquivo muito grande (máximo 10MB)' 
      });
    }
  }
  
  if (error.message === 'Apenas arquivos CSV são permitidos!') {
    return res.status(400).json({ 
      error: 'Apenas arquivos CSV são permitidos' 
    });
  }
  
  console.error('Erro não tratado:', error);
  res.status(500).json({ 
    error: 'Erro interno do servidor' 
  });
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log(`🚀 Servidor rodando na porta ${PORT}`);
  console.log(`📊 Acesse: http://localhost:${PORT}`);
});

module.exports = app;