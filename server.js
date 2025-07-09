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
  if (!priceString) return 0;
  
  // Remove "R$", espaços, pontos e vírgulas, converte para número
  const cleanPrice = priceString
    .replace(/R\$\s?/, '')
    .replace(/\./g, '')
    .replace(/,/g, '.');
  
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
          // Nomes das lojas na ordem correta
          const lojas = [
            'Loja da Suzy',
            'Loja Top Celulares',
            'Loja HUSSEIN',
            'Loja Paulo',
            'Loja HM',
            'Loja General',
            'Loja JR',
            'Loja Mega Cell'
          ];
          
          const processedData = [];
          
          // Processar cada loja
          lojas.forEach((loja, lojaIndex) => {
            const produtos = [];
            
            // Começar da linha 2 (índice 1) para pular cabeçalhos
            for (let i = 1; i < results.length; i++) {
              const row = results[i];
              const columns = Object.keys(row);
              
              let codigo, modelo, preco;
              
              // Determinar colunas baseado na posição da loja
              if (lojaIndex === 0) {
                // Primeira loja (Loja da Suzy)
                codigo = row[columns[0]];
                modelo = row[columns[1]];
                preco = row[columns[2]];
              } else {
                // Outras lojas - cada loja ocupa 4 colunas
                const baseCol = lojaIndex * 4;
                if (baseCol < columns.length) {
                  codigo = row[columns[baseCol]];
                  modelo = row[columns[baseCol + 1]];
                  preco = row[columns[baseCol + 2]];
                }
              }
              
              // Adicionar produto se tiver dados válidos
              if (codigo && modelo && preco && codigo.trim() !== '' && modelo.trim() !== '') {
                produtos.push({
                  codigo: codigo.trim(),
                  modelo: modelo.trim(),
                  preco: extractPrice(preco)
                });
              }
            }
            
            // Adicionar loja aos dados processados
            if (produtos.length > 0) {
              processedData.push({
                loja: loja,
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