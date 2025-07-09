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
  
  // Remove "R$", espaços, pontos (milhares) e troca vírgula por ponto
  const cleanPrice = priceString
    .toString()
    .replace(/R\$\s?/, '')
    .replace(/\./g, '')
    .replace(/,/g, '.')
    .trim();
  
  const price = parseFloat(cleanPrice);
  return isNaN(price) ? 0 : price;
}

// Função para processar o CSV com parser manual
function processCSV(filePath) {
  return new Promise((resolve, reject) => {
    try {
      // Ler arquivo como texto
      const fileContent = fs.readFileSync(filePath, 'utf8');
      
      // Dividir em linhas
      const lines = fileContent.split('\n').filter(line => line.trim());
      
      console.log('🔍 DEBUG: Total de linhas:', lines.length);
      console.log('🔍 DEBUG: Primeira linha:', lines[0]);
      console.log('🔍 DEBUG: Segunda linha:', lines[1]);
      
      if (lines.length < 2) {
        return resolve([]);
      }
      
      // Processar cabeçalho
      const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
      console.log('🔍 DEBUG: Cabeçalhos:', headers);
      
      // Configuração das lojas baseada nos cabeçalhos
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
      lojasConfig.forEach((lojaConfig, lojaIndex) => {
        console.log(`\n🔍 DEBUG: Processando ${lojaConfig.nome}...`);
        const produtos = [];
        
        // Processar linhas de dados (pular cabeçalho)
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i];
          if (!line || line.trim() === '') continue;
          
          // Dividir linha em colunas
          const columns = line.split(',').map(col => col.trim().replace(/"/g, ''));
          
          if (columns.length < 30) continue; // Linha incompleta
          
          const codigo = columns[lojaConfig.indices[0]];
          const modelo = columns[lojaConfig.indices[1]];
          const preco = columns[lojaConfig.indices[2]];
          
          // Debug para primeira loja nas primeiras linhas
          if (lojaIndex === 0 && i <= 5) {
            console.log(`🔍 DEBUG: Linha ${i}:`, {
              codigo: codigo,
              modelo: modelo,
              preco: preco
            });
          }
          
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