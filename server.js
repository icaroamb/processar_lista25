const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
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
  if (!priceString || priceString.trim() === '') return 0;
  
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

// Função para processar o CSV
function processCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        results.push(row);
      })
      .on('end', () => {
        try {
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
            const produtos = [];
            
            // Começar da linha 2 (índice 1) para pular cabeçalhos
            for (let i = 1; i < results.length; i++) {
              const row = results[i];
              
              const codigo = row[lojaConfig.colunas[0]];
              const modelo = row[lojaConfig.colunas[1]];
              const preco = row[lojaConfig.colunas[2]];
              
              // Adicionar produto se tiver dados válidos
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
          
          resolve(processedData);
        } catch (error) {
          reject(error);
        }
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

// Rota principal para upload e processamento
app.post('/process-csv', upload.single('csvFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ 
        error: 'Nenhum arquivo CSV foi enviado' 
      });
    }
    
    const filePath = req.file.path;
    
    // Processar o CSV
    const processedData = await processCSV(filePath);
    
    // Limpar arquivo temporário
    fs.unlinkSync(filePath);
    
    // Retornar dados processados
    res.json(processedData);
    
  } catch (error) {
    console.error('Erro ao processar CSV:', error);
    
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