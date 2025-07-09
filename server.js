const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Configurações do Bubble
const BUBBLE_CONFIG = {
  baseURL: 'https://calculaqui.com/api/1.1/obj',
  token: '7c4a6a50a83c872a298b261126781a8f',
  headers: {
    'token': '7c4a6a50a83c872a298b261126781a8f',
    'Content-Type': 'application/json'
  }
};

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
  
  result.push(current.trim());
  return result;
}

// Função para buscar dados do Bubble com paginação
async function fetchAllFromBubble(tableName, filters = {}) {
  try {
    console.log(`🔍 Buscando dados de ${tableName}...`);
    let allData = [];
    let cursor = 0;
    let hasMore = true;
    
    while (hasMore) {
      const params = { cursor, limit: 100, ...filters };
      
      const response = await axios.get(`${BUBBLE_CONFIG.baseURL}/${tableName}`, {
        headers: BUBBLE_CONFIG.headers,
        params,
        timeout: 30000
      });
      
      const data = response.data;
      
      if (!data.response || !data.response.results) {
        throw new Error(`Estrutura de resposta inválida para ${tableName}`);
      }
      
      allData = allData.concat(data.response.results);
      hasMore = data.response.remaining > 0;
      cursor = data.response.cursor || (cursor + 100);
    }
    
    console.log(`✅ ${tableName}: ${allData.length} registros carregados`);
    return allData;
    
  } catch (error) {
    console.error(`❌ Erro ao buscar ${tableName}:`, error.message);
    throw new Error(`Erro ao buscar ${tableName}: ${error.message}`);
  }
}

// Função para criar item no Bubble
async function createInBubble(tableName, data) {
  try {
    const response = await axios.post(`${BUBBLE_CONFIG.baseURL}/${tableName}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: 30000
    });
    return response.data;
  } catch (error) {
    console.error(`❌ Erro ao criar em ${tableName}:`, error.response?.data || error.message);
    throw new Error(`Erro ao criar em ${tableName}: ${error.response?.data?.body?.message || error.message}`);
  }
}

// Função para atualizar item no Bubble
async function updateInBubble(tableName, itemId, data) {
  try {
    const response = await axios.patch(`${BUBBLE_CONFIG.baseURL}/${tableName}/${itemId}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: 30000
    });
    return response.data;
  } catch (error) {
    console.error(`❌ Erro ao atualizar ${tableName}/${itemId}:`, error.response?.data || error.message);
    throw new Error(`Erro ao atualizar ${tableName}: ${error.response?.data?.body?.message || error.message}`);
  }
}

// Função para calcular estatísticas do produto baseadas no preco_final
function calculateProductStats(produtoFornecedores) {
  const validPrices = produtoFornecedores
    .filter(pf => pf.preco_final && pf.preco_final > 0)
    .map(pf => pf.preco_final);
  
  const qtd_fornecedores = validPrices.length;
  const menor_preco = qtd_fornecedores > 0 ? Math.min(...validPrices) : 0;
  const preco_medio = qtd_fornecedores > 0 ? validPrices.reduce((a, b) => a + b, 0) / qtd_fornecedores : 0;
  
  return { qtd_fornecedores, menor_preco, preco_medio };
}

// Função para processar o CSV
function processCSV(filePath) {
  return new Promise((resolve, reject) => {
    try {
      console.log('📁 Lendo arquivo CSV...');
      
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const lines = fileContent.split('\n').filter(line => line.trim());
      
      if (lines.length < 3) {
        console.log('❌ Arquivo CSV muito pequeno');
        return resolve([]);
      }
      
      // Pular as duas primeiras linhas (cabeçalhos)
      const dataLines = lines.slice(2);
      console.log(`📊 Processando ${dataLines.length} linhas de dados`);
      
      // Configuração das lojas com índices das colunas
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
      
      lojasConfig.forEach((lojaConfig) => {
        console.log(`🏪 Processando ${lojaConfig.nome}...`);
        const produtos = [];
        
        dataLines.forEach((line) => {
          if (!line || line.trim() === '') return;
          
          const columns = parseCSVLine(line);
          
          if (columns.length < 31) return;
          
          const codigo = columns[lojaConfig.indices[0]];
          const modelo = columns[lojaConfig.indices[1]];
          const preco = columns[lojaConfig.indices[2]];
          
          if (codigo && modelo && preco && 
              codigo.trim() !== '' && 
              modelo.trim() !== '' && 
              preco.trim() !== '') {
            
            const precoNumerico = extractPrice(preco);
            
            produtos.push({
              codigo: codigo.trim(),
              modelo: modelo.trim(),
              preco: precoNumerico
            });
          }
        });
        
        console.log(`✅ ${lojaConfig.nome}: ${produtos.length} produtos`);
        
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
      console.error('❌ Erro no processamento do CSV:', error);
      reject(error);
    }
  });
}

// Função principal para sincronizar com o Bubble
async function syncWithBubble(csvData, gorduraValor) {
  try {
    console.log('\n🔄 Iniciando sincronização com Bubble...');
    
    // 1. CARREGAR DADOS EXISTENTES
    console.log('📊 Carregando dados existentes...');
    const [fornecedores, produtos, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - fornecedor_25marco'),
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    console.log(`📊 Carregados: ${fornecedores.length} fornecedores, ${produtos.length} produtos, ${produtoFornecedores.length} relações`);
    
    // 2. CRIAR MAPAS PARA BUSCA RÁPIDA
    const fornecedorMap = new Map();
    fornecedores.forEach(f => fornecedorMap.set(f.nome_fornecedor, f));
    
    const produtoMap = new Map();
    produtos.forEach(p => produtoMap.set(p.id_planilha, p));
    
    const results = {
      fornecedores_criados: 0,
      produtos_criados: 0,
      relacoes_criadas: 0,
      relacoes_atualizadas: 0,
      relacoes_zeradas: 0
    };
    
    // 3. PROCESSAR PRODUTOS DO CSV
    console.log('\n📝 Processando produtos do CSV...');
    
    for (const lojaData of csvData) {
      console.log(`\n🏪 Processando ${lojaData.loja}...`);
      
      // 3.1 Verificar/criar fornecedor
      let fornecedor = fornecedorMap.get(lojaData.loja);
      if (!fornecedor) {
        console.log(`➕ Criando fornecedor: ${lojaData.loja}`);
        const novoFornecedor = await createInBubble('1 - fornecedor_25marco', {
          nome_fornecedor: lojaData.loja,
          status_ativo: 'yes'
        });
        fornecedor = { _id: novoFornecedor.id, nome_fornecedor: lojaData.loja };
        fornecedorMap.set(lojaData.loja, fornecedor);
        results.fornecedores_criados++;
      }
      
      // 3.2 Processar cada produto da loja
      for (const produtoCsv of lojaData.produtos) {
        // Verificar/criar produto
        let produto = produtoMap.get(produtoCsv.codigo);
        if (!produto) {
          console.log(`➕ Criando produto: ${produtoCsv.codigo}`);
          const novoProduto = await createInBubble('1 - produtos_25marco', {
            id_planilha: produtoCsv.codigo,
            nome_completo: produtoCsv.modelo,
            preco_medio: 0,
            qtd_fornecedores: 0,
            menor_preco: 0
          });
          produto = { 
            _id: novoProduto.id, 
            id_planilha: produtoCsv.codigo,
            nome_completo: produtoCsv.modelo
          };
          produtoMap.set(produtoCsv.codigo, produto);
          results.produtos_criados++;
        }
        
        // Calcular preços
        const precoOriginal = produtoCsv.preco;
        const precoFinal = precoOriginal === 0 ? 0 : precoOriginal + gorduraValor;
        const precoOrdenacao = precoOriginal === 0 ? 999999 : precoOriginal;
        
        // Verificar/criar/atualizar relação ProdutoFornecedor
        const relacaoExistente = produtoFornecedores.find(pf => 
          pf.produto === produto._id && pf.fornecedor === fornecedor._id
        );
        
        if (!relacaoExistente) {
          console.log(`➕ Criando relação: ${produtoCsv.codigo} - ${lojaData.loja}`);
          await createInBubble('1 - ProdutoFornecedor _25marco', {
            produto: produto._id,
            fornecedor: fornecedor._id,
            nome_produto: produtoCsv.modelo,
            preco_original: precoOriginal,
            preco_final: precoFinal,
            preco_ordenacao: precoOrdenacao,
            melhor_preco: false,
            status_ativo: 'yes'
          });
          results.relacoes_criadas++;
        } else if (relacaoExistente.preco_original !== precoOriginal) {
          console.log(`🔄 Atualizando relação: ${produtoCsv.codigo} - ${lojaData.loja}`);
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacaoExistente._id, {
            preco_original: precoOriginal,
            preco_final: precoFinal,
            preco_ordenacao: precoOrdenacao
          });
          results.relacoes_atualizadas++;
        }
      }
    }
    
    // 4. ZERAR PRODUTOS NÃO COTADOS (COTAÇÃO DIÁRIA)
    console.log('\n🧹 Aplicando lógica de cotação diária...');
    
    for (const lojaData of csvData) {
      const fornecedor = fornecedorMap.get(lojaData.loja);
      if (!fornecedor) continue;
      
      console.log(`🔍 Verificando produtos ausentes para: ${lojaData.loja}`);
      
      // Criar Set dos códigos cotados hoje
      const codigosCotadosHoje = new Set();
      lojaData.produtos.forEach(produto => {
        codigosCotadosHoje.add(produto.codigo);
      });
      
      console.log(`📋 Produtos cotados hoje: [${Array.from(codigosCotadosHoje).join(', ')}]`);
      
      // Buscar todas as relações existentes deste fornecedor
      const relacoesExistentes = produtoFornecedores.filter(pf => pf.fornecedor === fornecedor._id);
      
      for (const relacao of relacoesExistentes) {
        const produto = produtos.find(p => p._id === relacao.produto);
        if (!produto) continue;
        
        const codigoProduto = produto.id_planilha;
        const foiCotadoHoje = codigosCotadosHoje.has(codigoProduto);
        const temPreco = relacao.preco_original > 0;
        
        // Se produto NÃO foi cotado hoje MAS tinha preço, zerar
        if (!foiCotadoHoje && temPreco) {
          console.log(`🧹 Zerando produto ausente: ${codigoProduto} - ${lojaData.loja}`);
          
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacao._id, {
            preco_original: 0,
            preco_final: 0,
            preco_ordenacao: 999999
          });
          
          results.relacoes_zeradas++;
        }
      }
    }
    
    // 5. RECALCULAR ESTATÍSTICAS DOS PRODUTOS
    console.log('\n📊 Recalculando estatísticas dos produtos...');
    
    // Recarregar dados atualizados
    const produtoFornecedoresAtualizados = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco');
    
    // Agrupar por produto
    const produtoStats = new Map();
    produtoFornecedoresAtualizados.forEach(pf => {
      if (!produtoStats.has(pf.produto)) {
        produtoStats.set(pf.produto, []);
      }
      produtoStats.get(pf.produto).push(pf);
    });
    
    // Atualizar cada produto
    for (const [produtoId, relacoes] of produtoStats) {
      const stats = calculateProductStats(relacoes);
      
      // Atualizar estatísticas do produto
      await updateInBubble('1 - produtos_25marco', produtoId, {
        qtd_fornecedores: stats.qtd_fornecedores,
        menor_preco: stats.menor_preco,
        preco_medio: stats.preco_medio
      });
      
      // Atualizar melhor_preco nas relações (baseado no preco_final)
      for (const relacao of relacoes) {
        const isMelhorPreco = relacao.preco_final === stats.menor_preco && relacao.preco_final > 0;
        if (relacao.melhor_preco !== isMelhorPreco) {
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacao._id, {
            melhor_preco: isMelhorPreco
          });
        }
      }
    }
    
    console.log('\n✅ Sincronização concluída!');
    console.log('📊 Resultados:', results);
    
    return results;
    
  } catch (error) {
    console.error('❌ Erro na sincronização:', error);
    throw error;
  }
}

// ROTAS DA API

// Rota principal para upload e processamento
app.post('/process-csv', upload.single('csvFile'), async (req, res) => {
  try {
    console.log('\n🚀 === NOVA REQUISIÇÃO ===');
    console.log('📤 Arquivo:', req.file ? req.file.originalname : 'Nenhum');
    
    if (!req.file) {
      return res.status(400).json({ 
        error: 'Nenhum arquivo CSV foi enviado' 
      });
    }
    
    // Validar parâmetro gordura_valor
    const gorduraValor = parseFloat(req.body.gordura_valor);
    if (isNaN(gorduraValor)) {
      return res.status(400).json({
        error: 'Parâmetro gordura_valor é obrigatório e deve ser um número'
      });
    }
    
    console.log('💰 Gordura valor:', gorduraValor);
    
    const filePath = req.file.path;
    
    if (!fs.existsSync(filePath)) {
      return res.status(400).json({ 
        error: 'Arquivo não encontrado' 
      });
    }
    
    // Processar o CSV
    const csvData = await processCSV(filePath);
    
    // Sincronizar com Bubble
    const syncResults = await syncWithBubble(csvData, gorduraValor);
    
    // Limpar arquivo temporário
    fs.unlinkSync(filePath);
    console.log('🗑️ Arquivo temporário removido');
    
    console.log('✅ Processamento concluído com sucesso');
    
    // Retornar resultado
    res.json({
      success: true,
      message: 'CSV processado e sincronizado com sucesso',
      gordura_valor: gorduraValor,
      dados_csv: csvData,
      resultados_sincronizacao: syncResults
    });
    
  } catch (error) {
    console.error('❌ Erro ao processar CSV:', error);
    
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    
    res.status(500).json({ 
      error: 'Erro interno do servidor',
      details: error.message 
    });
  }
});

// Rota para buscar estatísticas
app.get('/stats', async (req, res) => {
  try {
    const [fornecedores, produtos, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - fornecedor_25marco'),
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    res.json({
      total_fornecedores: fornecedores.length,
      total_produtos: produtos.length,
      total_relacoes: produtoFornecedores.length,
      fornecedores_ativos: fornecedores.filter(f => f.status_ativo === 'yes').length,
      produtos_com_preco: produtos.filter(p => p.menor_preco > 0).length
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao buscar estatísticas',
      details: error.message
    });
  }
});

// Rota para buscar produto específico
app.get('/produto/:codigo', async (req, res) => {
  try {
    const codigo = req.params.codigo;
    
    const produtos = await fetchAllFromBubble('1 - produtos_25marco', {
      'id_planilha': codigo
    });
    
    if (produtos.length === 0) {
      return res.status(404).json({
        error: 'Produto não encontrado'
      });
    }
    
    const produto = produtos[0];
    
    // Buscar relações do produto
    const relacoes = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco', {
      'produto': produto._id
    });
    
    res.json({
      produto,
      fornecedores: relacoes.length,
      preco_menor: produto.menor_preco,
      preco_medio: produto.preco_medio
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao buscar produto',
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

// Rota para testar conectividade com Bubble
app.get('/test-bubble', async (req, res) => {
  try {
    console.log('🧪 Testando conectividade com Bubble...');
    
    const testResponse = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - fornecedor_25marco`, {
      headers: BUBBLE_CONFIG.headers,
      params: { limit: 1 },
      timeout: 10000
    });
    
    res.json({
      success: true,
      message: 'Conectividade com Bubble OK',
      bubble_response: testResponse.data,
      config: {
        baseURL: BUBBLE_CONFIG.baseURL,
        hasToken: !!BUBBLE_CONFIG.token
      }
    });
    
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Erro de conectividade com Bubble',
      details: {
        message: error.message,
        status: error.response?.status,
        data: error.response?.data
      }
    });
  }
});

// Rota de documentação
app.get('/', (req, res) => {
  res.json({
    message: 'API para processamento de CSV de produtos com integração Bubble',
    version: '3.0.0',
    endpoints: {
      'POST /process-csv': 'Envia arquivo CSV com parâmetro gordura_valor e sincroniza com Bubble',
      'GET /stats': 'Retorna estatísticas das tabelas',
      'GET /produto/:codigo': 'Busca produto específico por código',
      'GET /health': 'Verifica status da API',
      'GET /test-bubble': 'Testa conectividade com Bubble'
    },
    parametros_obrigatorios: {
      'gordura_valor': 'number - Valor a ser adicionado ao preço original'
    },
    funcionalidades: [
      'Processamento de CSV com layout horizontal',
      'Cotação diária completa (zera produtos não cotados)',
      'Cálculos baseados no preço final (com margem)',
      'Identificação automática do melhor preço',
      'Sincronização inteligente com Bubble'
    ]
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
  console.log(`🔗 Integração Bubble configurada`);
  console.log(`✨ Versão 3.0.0 - Código reescrito do zero`);
});

module.exports = app;