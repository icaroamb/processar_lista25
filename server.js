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

// Função para fazer parse correto do CSV respeitando aspas
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  
  // Adicionar último campo
  result.push(current.trim());
  
  return result;
}

// Função para processar o CSV de forma super simples
function processCSV(filePath) {
  return new Promise((resolve, reject) => {
    try {
      console.log('📁 Lendo arquivo:', filePath);
      
      // Ler arquivo como texto
      const fileContent = fs.readFileSync(filePath, 'utf8');
      console.log('📄 Arquivo lido, tamanho:', fileContent.length, 'caracteres');
      
      // Dividir em linhas
      const lines = fileContent.split('\n').filter(line => line.trim());
      console.log('📋 Total de linhas:', lines.length);
      
      if (lines.length < 2) {
        console.log('❌ Arquivo muito pequeno');
        return resolve([]);
      }
      
      // Processar cabeçalho para debug
      const headerColumns = parseCSVLine(lines[0]);
      console.log('📄 Cabeçalho tem', headerColumns.length, 'colunas');
      console.log('🔍 Primeiros cabeçalhos:', headerColumns.slice(0, 10));
      
      // Verificar se a segunda linha também é cabeçalho
      if (lines.length > 1) {
        const secondLine = parseCSVLine(lines[1]);
        console.log('🔍 Segunda linha:', secondLine.slice(0, 6));
      }
      
      // Pular as duas primeiras linhas (cabeçalhos)
      const dataLines = lines.slice(2);
      console.log('📊 Linhas de dados:', dataLines.length);
      
      // Configuração das lojas com índices fixos das colunas
      const lojasConfig = [
        { nome: 'Loja da Suzy', indices: [0, 1, 2] },
        { nome: 'Loja Top Celulares', indices: [4, 5, 6] },
        { nome: 'Loja HUSSEIN', indices: [8, 9, 10] },
        { nome: 'Loja Paulo', indices: [12, 13, 14] },
        { nome: 'Loja HM', indices: [16, 17, 18] },
        { nome: 'Loja General', indices: [20, 21, 22] },
        { nome: 'Loja JR', indices: [24, 25, 26] },
        { nome: 'Loja Mega Cell', indices: [28, 29, 30] }
      ];
      
      const processedData = [];
      
      // Processar cada loja
      lojasConfig.forEach((lojaConfig) => {
        console.log(`\n🏪 Processando ${lojaConfig.nome}...`);
        const produtos = [];
        
        // Processar cada linha de dados
        dataLines.forEach((line, lineIndex) => {
          if (!line || line.trim() === '') return;
          
          // Parse correto da linha CSV
          const columns = parseCSVLine(line);
          
          if (columns.length < 31) {
            console.log(`⚠️  Linha ${lineIndex + 2} muito curta: ${columns.length} colunas`);
            return;
          }
          
          const codigo = columns[lojaConfig.indices[0]];
          const modelo = columns[lojaConfig.indices[1]];
          const preco = columns[lojaConfig.indices[2]];
          
          // Debug para primeira loja nas primeiras linhas
          if (lojaConfig.nome === 'Loja da Suzy' && lineIndex < 3) {
            console.log(`🔍 Linha ${lineIndex + 2}:`, {
              codigo: codigo,
              modelo: modelo,
              preco: preco
            });
          }
          
          // Verificar se tem dados válidos
          if (codigo && modelo && preco && 
              codigo.trim() !== '' && 
              modelo.trim() !== '' && 
              preco.trim() !== '') {
            
            const precoNumerico = extractPrice(preco);
            
            if (precoNumerico > 0) {
              produtos.push({
                codigo: codigo.trim(),
                modelo: modelo.trim(),
                preco: precoNumerico
              });
            }
          }
        });
        
        console.log(`✅ ${lojaConfig.nome}: ${produtos.length} produtos encontrados`);
        
        // Mostrar alguns produtos para debug
        if (produtos.length > 0) {
          console.log('🔍 Primeiros 2 produtos:');
          produtos.slice(0, 2).forEach(p => {
            console.log(`   ${p.codigo} | ${p.modelo} | R$ ${p.preco}`);
          });
        }
        
        // Adicionar loja aos dados processados
        if (produtos.length > 0) {
          processedData.push({
            loja: lojaConfig.nome,
            total_produtos: produtos.length,
            produtos: produtos
          });
        }
      });
      
      console.log('\n📈 RESUMO FINAL:');
      console.log('Total de lojas processadas:', processedData.length);
      processedData.forEach(loja => {
        console.log(`  ${loja.loja}: ${loja.total_produtos} produtos`);
      });
      
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
    console.log('\n🚀 === NOVA REQUISIÇÃO ===');
    console.log('📤 Arquivo recebido:', req.file ? req.file.originalname : 'Nenhum');
    
    if (!req.file) {
      console.log('❌ Nenhum arquivo enviado');
      return res.status(400).json({ 
        error: 'Nenhum arquivo CSV foi enviado' 
      });
    }
    
    const filePath = req.file.path;
    console.log('📁 Caminho do arquivo:', filePath);
    
    // Verificar se o arquivo existe
    if (!fs.existsSync(filePath)) {
      console.log('❌ Arquivo não encontrado');
      return res.status(400).json({ 
        error: 'Arquivo não encontrado' 
      });
    }
    
    // Processar o CSV
    const processedData = await processCSV(filePath);
    
    // Limpar arquivo temporário
    fs.unlinkSync(filePath);
    console.log('🗑️  Arquivo temporário removido');
    
    console.log('✅ Processamento concluído');
    console.log('📊 Retornando dados de', processedData.length, 'lojas');
    
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