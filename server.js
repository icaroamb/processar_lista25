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
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// Configurações do Bubble
const BUBBLE_CONFIG = {
  baseURL: 'https://calculaqui.com/api/1.1/obj',
  token: '7c4a6a50a83c872a298b261126781a8f',
  headers: {
    'token': '7c4a6a50a83c872a298b261126781a8f',
    'Content-Type': 'application/json'
  }
};

// Configurações de processamento para alto volume
const PROCESSING_CONFIG = {
  BATCH_SIZE: 50,           // Tamanho do lote para processamento
  MAX_CONCURRENT: 5,        // Máximo de operações simultâneas
  RETRY_ATTEMPTS: 3,        // Tentativas de retry
  RETRY_DELAY: 1000,        // Delay entre tentativas (ms)
  REQUEST_TIMEOUT: 60000,   // Timeout por requisição (60s)
  BATCH_DELAY: 100,         // Delay entre lotes (ms)
  MEMORY_CLEANUP_INTERVAL: 1000 // Interval para limpeza de memória
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
    fileSize: 100 * 1024 * 1024 // 100MB max para arquivos grandes
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

// Função de delay para evitar sobrecarga
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Função de retry para operações críticas
async function retryOperation(operation, maxAttempts = PROCESSING_CONFIG.RETRY_ATTEMPTS) {
  let lastError;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      console.warn(`⚠️ Tentativa ${attempt}/${maxAttempts} falhou:`, error.message);
      
      if (attempt < maxAttempts) {
        const delayTime = PROCESSING_CONFIG.RETRY_DELAY * attempt;
        await delay(delayTime);
      }
    }
  }
  
  throw lastError;
}

// Função para buscar dados do Bubble com correção do loop infinito
async function fetchAllFromBubble(tableName, filters = {}) {
  try {
    console.log(`🔍 Buscando dados de ${tableName}...`);
    let allData = [];
    let cursor = 0;
    let hasMore = true;
    let totalFetched = 0;
    let maxIterations = 1000; // Proteção contra loop infinito
    let currentIteration = 0;
    
    while (hasMore && currentIteration < maxIterations) {
      currentIteration++;
      
      const params = { cursor, limit: 100, ...filters };
      
      const response = await retryOperation(async () => {
        return await axios.get(`${BUBBLE_CONFIG.baseURL}/${tableName}`, {
          headers: BUBBLE_CONFIG.headers,
          params,
          timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
        });
      });
      
      const data = response.data;
      
      if (!data.response || !data.response.results) {
        throw new Error(`Estrutura de resposta inválida para ${tableName}`);
      }
      
      const newResults = data.response.results;
      
      // Se não há novos resultados, sair do loop
      if (!newResults || newResults.length === 0) {
        console.log(`📊 ${tableName}: Nenhum novo resultado encontrado, finalizando busca`);
        break;
      }
      
      allData = allData.concat(newResults);
      totalFetched += newResults.length;
      
      // Verificar se há mais dados usando múltiplas condições
      const remaining = data.response.remaining || 0;
      const newCursor = data.response.cursor;
      
      hasMore = remaining > 0 && newCursor && newCursor !== cursor;
      
      if (hasMore) {
        cursor = newCursor;
      }
      
      console.log(`📊 ${tableName}: ${totalFetched} registros carregados (restam: ${remaining}, cursor: ${cursor})`);
      
      // Pequeno delay para evitar rate limiting
      if (hasMore) {
        await delay(50);
      }
      
      // Proteção adicional: se o cursor não mudou, sair do loop
      if (newCursor === cursor && remaining > 0) {
        console.warn(`⚠️ ${tableName}: Cursor não mudou, possível loop infinito detectado. Finalizando busca.`);
        break;
      }
    }
    
    if (currentIteration >= maxIterations) {
      console.warn(`⚠️ ${tableName}: Atingido limite máximo de iterações (${maxIterations}). Possível loop infinito.`);
    }
    
    console.log(`✅ ${tableName}: ${allData.length} registros carregados (total em ${currentIteration} iterações)`);
    return allData;
    
  } catch (error) {
    console.error(`❌ Erro ao buscar ${tableName}:`, error.message);
    throw new Error(`Erro ao buscar ${tableName}: ${error.message}`);
  }
}

// Função para criar item no Bubble com retry
async function createInBubble(tableName, data) {
  return await retryOperation(async () => {
    const response = await axios.post(`${BUBBLE_CONFIG.baseURL}/${tableName}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
    });
    return response.data;
  });
}

// Função para atualizar item no Bubble com retry
async function updateInBubble(tableName, itemId, data) {
  return await retryOperation(async () => {
    const response = await axios.patch(`${BUBBLE_CONFIG.baseURL}/${tableName}/${itemId}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
    });
    return response.data;
  });
}

// Função para calcular estatísticas do produto baseadas no preco_final - CORRIGIDA
function calculateProductStats(produtoFornecedores) {
  console.log(`📊 Calculando stats para produto com ${produtoFornecedores.length} relações`);
  
  // Filtrar apenas relações com preço válido (> 0) e status ativo
  const validPrices = produtoFornecedores
    .filter(pf => {
      const isValid = pf.preco_final && 
                     pf.preco_final > 0 && 
                     pf.status_ativo === 'yes';
      
      if (!isValid) {
        console.log(`📊 Relação inválida: preco_final=${pf.preco_final}, status=${pf.status_ativo}`);
      }
      
      return isValid;
    })
    .map(pf => pf.preco_final);
  
  console.log(`📊 Preços válidos encontrados: [${validPrices.join(', ')}]`);
  
  const qtd_fornecedores = validPrices.length;
  const menor_preco = qtd_fornecedores > 0 ? Math.min(...validPrices) : 0;
  const preco_medio = qtd_fornecedores > 0 ? 
    Math.round((validPrices.reduce((a, b) => a + b, 0) / qtd_fornecedores) * 100) / 100 : 0;
  
  console.log(`📊 Stats calculadas: qtd=${qtd_fornecedores}, menor=${menor_preco}, media=${preco_medio}`);
  
  return { qtd_fornecedores, menor_preco, preco_medio };
}

// Função otimizada para processar o CSV
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
        
        // Processar em chunks para economizar memória
        const chunkSize = 1000;
        for (let i = 0; i < dataLines.length; i += chunkSize) {
          const chunk = dataLines.slice(i, i + chunkSize);
          
          chunk.forEach((line) => {
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
          
          // Forçar garbage collection a cada chunk
          if (global.gc && i % (chunkSize * 5) === 0) {
            global.gc();
          }
        }
        
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

// Função para processar lotes com controle de concorrência
async function processBatch(items, processorFunction, batchSize = PROCESSING_CONFIG.BATCH_SIZE) {
  const results = [];
  const errors = [];
  
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    console.log(`📦 Processando lote ${Math.floor(i/batchSize) + 1}/${Math.ceil(items.length/batchSize)} (${batch.length} itens)`);
    
    // Processar lote com controle de concorrência
    const promises = batch.map(async (item, index) => {
      try {
        const result = await processorFunction(item, i + index);
        return { success: true, result, index: i + index };
      } catch (error) {
        console.error(`❌ Erro no item ${i + index}:`, error.message);
        errors.push({ index: i + index, error: error.message, item });
        return { success: false, error: error.message, index: i + index };
      }
    });
    
    // Limitar concorrência
    const concurrentPromises = [];
    for (let j = 0; j < promises.length; j += PROCESSING_CONFIG.MAX_CONCURRENT) {
      const concurrentBatch = promises.slice(j, j + PROCESSING_CONFIG.MAX_CONCURRENT);
      concurrentPromises.push(Promise.all(concurrentBatch));
    }
    
    const batchResults = await Promise.all(concurrentPromises);
    results.push(...batchResults.flat());
    
    // Delay entre lotes para evitar sobrecarga
    if (i + batchSize < items.length) {
      await delay(PROCESSING_CONFIG.BATCH_DELAY);
    }
    
    // Limpeza de memória periódica
    if (global.gc && (i + batchSize) % PROCESSING_CONFIG.MEMORY_CLEANUP_INTERVAL === 0) {
      global.gc();
    }
  }
  
  return { results, errors };
}

// Função principal para sincronizar com o Bubble - OTIMIZADA
async function syncWithBubble(csvData, gorduraValor) {
  try {
    console.log('\n🔄 Iniciando sincronização otimizada com Bubble...');
    
    // 1. CARREGAR DADOS EXISTENTES
    console.log('📊 Carregando dados existentes...');
    const [fornecedores, produtos, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - fornecedor_25marco'),
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    console.log(`📊 Carregados: ${fornecedores.length} fornecedores, ${produtos.length} produtos, ${produtoFornecedores.length} relações`);
    
    // 2. CRIAR MAPAS OTIMIZADOS PARA BUSCA RÁPIDA
    const fornecedorMap = new Map();
    fornecedores.forEach(f => fornecedorMap.set(f.nome_fornecedor, f));
    
    const produtoMap = new Map();
    produtos.forEach(p => produtoMap.set(p.id_planilha, p));
    
    const relacaoMap = new Map();
    produtoFornecedores.forEach(pf => {
      relacaoMap.set(`${pf.produto}-${pf.fornecedor}`, pf);
    });
    
    const results = {
      fornecedores_criados: 0,
      produtos_criados: 0,
      relacoes_criadas: 0,
      relacoes_atualizadas: 0,
      relacoes_zeradas: 0,
      erros: []
    };
    
    // 3. PREPARAR TODAS AS OPERAÇÕES EM MEMÓRIA PRIMEIRO - COM VERIFICAÇÃO DE DUPLICATAS
    console.log('\n📝 Preparando operações...');
    const operacoesFornecedores = [];
    const operacoesProdutos = [];
    const operacoesRelacoes = [];
    
    // Sets para evitar duplicatas nas operações
    const fornecedoresParaCriar = new Set();
    const produtosParaCriar = new Set();
    
    // Coletar todos os códigos cotados por fornecedor para lógica de cotação diária
    const codigosCotadosPorFornecedor = new Map();
    
    for (const lojaData of csvData) {
      const codigosCotados = new Set();
      
      // 3.1 Verificar fornecedor (evitar duplicatas)
      if (!fornecedorMap.has(lojaData.loja) && !fornecedoresParaCriar.has(lojaData.loja)) {
        fornecedoresParaCriar.add(lojaData.loja);
        operacoesFornecedores.push({
          tipo: 'criar',
          nome: lojaData.loja,
          dados: {
            nome_fornecedor: lojaData.loja,
            status_ativo: 'yes'
          }
        });
      }
      
      // 3.2 Processar produtos da loja (evitar duplicatas)
      for (const produtoCsv of lojaData.produtos) {
        codigosCotados.add(produtoCsv.codigo);
        
        // Verificar produto (evitar duplicatas)
        if (!produtoMap.has(produtoCsv.codigo) && !produtosParaCriar.has(produtoCsv.codigo)) {
          produtosParaCriar.add(produtoCsv.codigo);
          operacoesProdutos.push({
            tipo: 'criar',
            codigo: produtoCsv.codigo,
            dados: {
              id_planilha: produtoCsv.codigo,
              nome_completo: produtoCsv.modelo,
              preco_medio: 0,
              qtd_fornecedores: 0,
              menor_preco: 0
            }
          });
        }
        
        // Calcular preços
        const precoOriginal = produtoCsv.preco;
        const precoFinal = precoOriginal === 0 ? 0 : precoOriginal + gorduraValor;
        const precoOrdenacao = precoOriginal === 0 ? 999999 : precoOriginal;
        
        // Preparar operação de relação
        operacoesRelacoes.push({
          tipo: 'processar',
          loja: lojaData.loja,
          codigo: produtoCsv.codigo,
          modelo: produtoCsv.modelo,
          precoOriginal,
          precoFinal,
          precoOrdenacao
        });
      }
      
      codigosCotadosPorFornecedor.set(lojaData.loja, codigosCotados);
    }
    
    console.log(`📋 Operações preparadas: ${operacoesFornecedores.length} fornecedores únicos, ${operacoesProdutos.length} produtos únicos, ${operacoesRelacoes.length} relações`);
    
    // 4. EXECUTAR OPERAÇÕES EM LOTES
    
    // 4.1 Criar fornecedores em lotes - COM VERIFICAÇÃO DE DUPLICAÇÃO
    if (operacoesFornecedores.length > 0) {
      console.log('\n👥 Criando fornecedores...');
      
      // Verificar se fornecedores já existem antes de criar
      const fornecedoresParaCriar = [];
      
      for (const operacao of operacoesFornecedores) {
        // Verificação dupla: no mapa local E busca no Bubble
        if (!fornecedorMap.has(operacao.nome)) {
          // Buscar no Bubble para garantir que não existe
          try {
            const fornecedorExistente = await fetchAllFromBubble('1 - fornecedor_25marco', {
              'constraints': [{
                'key': 'nome_fornecedor',
                'constraint_type': 'equals',
                'value': operacao.nome
              }]
            });
            
            if (fornecedorExistente.length === 0) {
              fornecedoresParaCriar.push(operacao);
            } else {
              // Fornecedor já existe, adicionar ao mapa
              const fornecedor = fornecedorExistente[0];
              fornecedorMap.set(operacao.nome, {
                _id: fornecedor._id,
                nome_fornecedor: fornecedor.nome_fornecedor
              });
              console.log(`📋 Fornecedor ${operacao.nome} já existe, pulando criação`);
            }
          } catch (error) {
            console.warn(`⚠️ Erro ao verificar fornecedor ${operacao.nome}:`, error.message);
            fornecedoresParaCriar.push(operacao); // Em caso de erro, tentar criar
          }
        }
        
        // Pequeno delay para evitar sobrecarga
        await delay(10);
      }
      
      console.log(`👥 Fornecedores únicos para criar: ${fornecedoresParaCriar.length} de ${operacoesFornecedores.length}`);
      
      if (fornecedoresParaCriar.length > 0) {
        const { results: fornecedorResults, errors: fornecedorErrors } = await processBatch(
          fornecedoresParaCriar,
          async (operacao) => {
            // Verificação final antes de criar
            if (fornecedorMap.has(operacao.nome)) {
              return { skipped: true, nome: operacao.nome };
            }
            
            const novoFornecedor = await createInBubble('1 - fornecedor_25marco', operacao.dados);
            fornecedorMap.set(operacao.dados.nome_fornecedor, {
              _id: novoFornecedor.id,
              nome_fornecedor: operacao.dados.nome_fornecedor
            });
            return novoFornecedor;
          }
        );
        results.fornecedores_criados = fornecedorResults.filter(r => r.success && !r.result?.skipped).length;
        results.erros.push(...fornecedorErrors);
      }
    }
    
    // 4.2 Criar produtos em lotes - COM VERIFICAÇÃO DE DUPLICAÇÃO
    if (operacoesProdutos.length > 0) {
      console.log('\n📦 Criando produtos...');
      
      // Verificar se produtos já existem antes de criar
      const produtosParaCriar = [];
      
      for (const operacao of operacoesProdutos) {
        // Verificação dupla: no mapa local E busca no Bubble
        if (!produtoMap.has(operacao.codigo)) {
          // Buscar no Bubble para garantir que não existe
          try {
            const produtoExistente = await fetchAllFromBubble('1 - produtos_25marco', {
              'constraints': [{
                'key': 'id_planilha',
                'constraint_type': 'equals',
                'value': operacao.codigo
              }]
            });
            
            if (produtoExistente.length === 0) {
              produtosParaCriar.push(operacao);
            } else {
              // Produto já existe, adicionar ao mapa
              const produto = produtoExistente[0];
              produtoMap.set(operacao.codigo, {
                _id: produto._id,
                id_planilha: produto.id_planilha,
                nome_completo: produto.nome_completo
              });
              console.log(`📋 Produto ${operacao.codigo} já existe, pulando criação`);
            }
          } catch (error) {
            console.warn(`⚠️ Erro ao verificar produto ${operacao.codigo}:`, error.message);
            produtosParaCriar.push(operacao); // Em caso de erro, tentar criar
          }
        }
        
        // Pequeno delay para evitar sobrecarga
        await delay(10);
      }
      
      console.log(`📦 Produtos únicos para criar: ${produtosParaCriar.length} de ${operacoesProdutos.length}`);
      
      if (produtosParaCriar.length > 0) {
        const { results: produtoResults, errors: produtoErrors } = await processBatch(
          produtosParaCriar,
          async (operacao) => {
            // Verificação final antes de criar
            if (produtoMap.has(operacao.codigo)) {
              return { skipped: true, codigo: operacao.codigo };
            }
            
            const novoProduto = await createInBubble('1 - produtos_25marco', operacao.dados);
            produtoMap.set(operacao.codigo, {
              _id: novoProduto.id,
              id_planilha: operacao.codigo,
              nome_completo: operacao.dados.nome_completo
            });
            return novoProduto;
          }
        );
        results.produtos_criados = produtoResults.filter(r => r.success && !r.result?.skipped).length;
        results.erros.push(...produtoErrors);
      }
    }
    
    // 4.3 Processar relações em lotes
    console.log('\n🔗 Processando relações...');
    const { results: relacaoResults, errors: relacaoErrors } = await processBatch(
      operacoesRelacoes,
      async (operacao) => {
        const fornecedor = fornecedorMap.get(operacao.loja);
        const produto = produtoMap.get(operacao.codigo);
        
        if (!fornecedor || !produto) {
          throw new Error(`Fornecedor ou produto não encontrado: ${operacao.loja} - ${operacao.codigo}`);
        }
        
        const chaveRelacao = `${produto._id}-${fornecedor._id}`;
        const relacaoExistente = relacaoMap.get(chaveRelacao);
        
        if (!relacaoExistente) {
          const novaRelacao = await createInBubble('1 - ProdutoFornecedor _25marco', {
            produto: produto._id,
            fornecedor: fornecedor._id,
            nome_produto: operacao.modelo,
            preco_original: operacao.precoOriginal,
            preco_final: operacao.precoFinal,
            preco_ordenacao: operacao.precoOrdenacao,
            melhor_preco: false,
            status_ativo: 'yes'
          });
          return { tipo: 'criada', resultado: novaRelacao };
        } else if (relacaoExistente.preco_original !== operacao.precoOriginal) {
          const relacaoAtualizada = await updateInBubble('1 - ProdutoFornecedor _25marco', relacaoExistente._id, {
            preco_original: operacao.precoOriginal,
            preco_final: operacao.precoFinal,
            preco_ordenacao: operacao.precoOrdenacao
          });
          return { tipo: 'atualizada', resultado: relacaoAtualizada };
        }
        
        return { tipo: 'inalterada' };
      }
    );
    
    results.relacoes_criadas = relacaoResults.filter(r => r.success && r.result?.tipo === 'criada').length;
    results.relacoes_atualizadas = relacaoResults.filter(r => r.success && r.result?.tipo === 'atualizada').length;
    results.erros.push(...relacaoErrors);
    
    // 4.4 APLICAR LÓGICA DE COTAÇÃO DIÁRIA
    console.log('\n🧹 Aplicando lógica de cotação diária...');
    const operacoesZeramento = [];
    
    for (const [lojaName, codigosCotadosHoje] of codigosCotadosPorFornecedor) {
      const fornecedor = fornecedorMap.get(lojaName);
      if (!fornecedor) continue;
      
      const relacoesExistentes = produtoFornecedores.filter(pf => pf.fornecedor === fornecedor._id);
      
      for (const relacao of relacoesExistentes) {
        const produto = produtos.find(p => p._id === relacao.produto);
        if (!produto) continue;
        
        const codigoProduto = produto.id_planilha;
        const foiCotadoHoje = codigosCotadosHoje.has(codigoProduto);
        const temPreco = relacao.preco_original > 0;
        
        if (!foiCotadoHoje && temPreco) {
          operacoesZeramento.push({
            relacaoId: relacao._id,
            codigo: codigoProduto,
            loja: lojaName
          });
        }
      }
    }
    
    if (operacoesZeramento.length > 0) {
      console.log(`🧹 Zerando ${operacoesZeramento.length} produtos não cotados...`);
      const { results: zeramentoResults, errors: zeramentoErrors } = await processBatch(
        operacoesZeramento,
        async (operacao) => {
          return await updateInBubble('1 - ProdutoFornecedor _25marco', operacao.relacaoId, {
            preco_original: 0,
            preco_final: 0,
            preco_ordenacao: 999999
          });
        }
      );
      results.relacoes_zeradas = zeramentoResults.filter(r => r.success).length;
      results.erros.push(...zeramentoErrors);
    }
    
    // 5. RECALCULAR ESTATÍSTICAS DOS PRODUTOS - COMPLETAMENTE REESCRITO
    console.log('\n📊 Recalculando estatísticas dos produtos...');
    
    // Recarregar TODOS os dados atualizados
    console.log('🔄 Recarregando dados atualizados...');
    const [produtosAtualizados, produtoFornecedoresAtualizados] = await Promise.all([
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    console.log(`📊 Dados recarregados: ${produtosAtualizados.length} produtos, ${produtoFornecedoresAtualizados.length} relações`);
    
    // Agrupar relações por produto de forma otimizada
    const produtoStatsMap = new Map();
    
    produtoFornecedoresAtualizados.forEach(pf => {
      if (!produtoStatsMap.has(pf.produto)) {
        produtoStatsMap.set(pf.produto, []);
      }
      produtoStatsMap.get(pf.produto).push(pf);
    });
    
    console.log(`📊 Agrupadas relações para ${produtoStatsMap.size} produtos únicos`);
    
    // Preparar operações de atualização de estatísticas e melhor preço
    const operacoesEstatisticas = [];
    const operacoesMelhorPreco = [];
    let produtosComPreco = 0;
    let produtosSemPreco = 0;
    
    for (const [produtoId, relacoes] of produtoStatsMap) {
      console.log(`\n📊 Processando produto ID: ${produtoId} com ${relacoes.length} relações`);
      
      // Calcular estatísticas
      const stats = calculateProductStats(relacoes);
      
      if (stats.qtd_fornecedores > 0) {
        produtosComPreco++;
        console.log(`✅ Produto com preço: qtd=${stats.qtd_fornecedores}, menor=${stats.menor_preco}, media=${stats.preco_medio}`);
      } else {
        produtosSemPreco++;
        console.log(`❌ Produto sem preço válido`);
      }
      
      // Adicionar à lista de atualizações de estatísticas
      operacoesEstatisticas.push({
        produtoId,
        stats
      });
      
      // Determinar melhor preço e preparar atualizações
      relacoes.forEach(relacao => {
        // Lógica corrigida para melhor preço:
        // 1. Deve ter preço > 0
        // 2. Deve ser o menor preço entre todos os fornecedores
        // 3. Deve estar ativo
        const isMelhorPreco = relacao.preco_final > 0 && 
                             relacao.preco_final === stats.menor_preco && 
                             relacao.status_ativo === 'yes' &&
                             stats.menor_preco > 0;
        
        // Só atualizar se o valor atual estiver diferente
        if (relacao.melhor_preco !== isMelhorPreco) {
          operacoesMelhorPreco.push({
            relacaoId: relacao._id,
            melhorPreco: isMelhorPreco,
            precoFinal: relacao.preco_final,
            menorPreco: stats.menor_preco
          });
          
          console.log(`🏆 Melhor preço para relação ${relacao._id}: ${isMelhorPreco} (preço: ${relacao.preco_final}, menor: ${stats.menor_preco})`);
        }
      });
    }
    
    console.log(`\n📊 Resumo: ${produtosComPreco} produtos com preço, ${produtosSemPreco} sem preço`);
    console.log(`📊 Operações preparadas: ${operacoesEstatisticas.length} atualizações de stats, ${operacoesMelhorPreco.length} atualizações de melhor preço`);
    
    // Executar atualizações de estatísticas em lotes
    if (operacoesEstatisticas.length > 0) {
      console.log(`\n📊 Atualizando estatísticas de ${operacoesEstatisticas.length} produtos...`);
      const { results: statsResults, errors: statsErrors } = await processBatch(
        operacoesEstatisticas,
        async (operacao) => {
          console.log(`📊 Atualizando produto ${operacao.produtoId}: qtd=${operacao.stats.qtd_fornecedores}, menor=${operacao.stats.menor_preco}, media=${operacao.stats.preco_medio}`);
          
          return await updateInBubble('1 - produtos_25marco', operacao.produtoId, {
            qtd_fornecedores: operacao.stats.qtd_fornecedores,
            menor_preco: operacao.stats.menor_preco,
            preco_medio: operacao.stats.preco_medio
          });
        }
      );
      
      const sucessoStats = statsResults.filter(r => r.success).length;
      console.log(`✅ Estatísticas atualizadas: ${sucessoStats}/${operacoesEstatisticas.length}`);
      results.erros.push(...statsErrors);
    }
    
    // Executar atualizações de melhor preço em lotes
    if (operacoesMelhorPreco.length > 0) {
      console.log(`\n🏆 Atualizando melhor preço de ${operacoesMelhorPreco.length} relações...`);
      const { results: melhorPrecoResults, errors: melhorPrecoErrors } = await processBatch(
        operacoesMelhorPreco,
        async (operacao) => {
          console.log(`🏆 Atualizando relação ${operacao.relacaoId}: melhor_preco=${operacao.melhorPreco} (${operacao.precoFinal} vs ${operacao.menorPreco})`);
          
          return await updateInBubble('1 - ProdutoFornecedor _25marco', operacao.relacaoId, {
            melhor_preco: operacao.melhorPreco
          });
        }
      );
      
      const sucessoMelhorPreco = melhorPrecoResults.filter(r => r.success).length;
      console.log(`✅ Melhor preço atualizado: ${sucessoMelhorPreco}/${operacoesMelhorPreco.length}`);
      results.erros.push(...melhorPrecoErrors);
    }
    
    // Aguardar um momento para propagação das mudanças
    await delay(1000);
    
    console.log('\n✅ Sincronização otimizada concluída!');
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
    console.log('📊 Tamanho do arquivo:', (req.file.size / 1024 / 1024).toFixed(2) + ' MB');
    
    const filePath = req.file.path;
    
    if (!fs.existsSync(filePath)) {
      return res.status(400).json({ 
        error: 'Arquivo não encontrado' 
      });
    }
    
    // Processar o CSV
    console.log('⏱️ Iniciando processamento otimizado...');
    const startTime = Date.now();
    
    const csvData = await processCSV(filePath);
    
    // Sincronizar com Bubble
    const syncResults = await syncWithBubble(csvData, gorduraValor);
    
    const endTime = Date.now();
    const processingTime = (endTime - startTime) / 1000;
    
    // Limpar arquivo temporário
    fs.unlinkSync(filePath);
    console.log('🗑️ Arquivo temporário removido');
    
    console.log(`✅ Processamento concluído em ${processingTime}s`);
    
    // Retornar resultado
    res.json({
      success: true,
      message: 'CSV processado e sincronizado com sucesso',
      gordura_valor: gorduraValor,
      tempo_processamento: processingTime + 's',
      tamanho_arquivo: (req.file.size / 1024 / 1024).toFixed(2) + ' MB',
      dados_csv: csvData.map(loja => ({
        loja: loja.loja,
        total_produtos: loja.total_produtos
      })),
      resultados_sincronizacao: syncResults,
      estatisticas_processamento: {
        total_lojas_processadas: csvData.length,
        total_produtos_csv: csvData.reduce((acc, loja) => acc + loja.total_produtos, 0),
        erros_encontrados: syncResults.erros.length
      }
    });
    
  } catch (error) {
    console.error('❌ Erro ao processar CSV:', error);
    
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    
    res.status(500).json({ 
      error: 'Erro interno do servidor',
      details: error.message,
      timestamp: new Date().toISOString()
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
      produtos_com_preco: produtos.filter(p => p.menor_preco > 0).length,
      relacoes_ativas: produtoFornecedores.filter(pf => pf.status_ativo === 'yes').length,
      relacoes_com_preco: produtoFornecedores.filter(pf => pf.preco_final > 0).length,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao buscar estatísticas',
      details: error.message
    });
  }
});

// Rota para buscar produto específico - COM DEBUGGING
app.get('/produto/:codigo', async (req, res) => {
  try {
    const codigo = req.params.codigo;
    console.log(`🔍 Buscando produto: ${codigo}`);
    
    const produtos = await fetchAllFromBubble('1 - produtos_25marco', {
      'constraints': [{
        'key': 'id_planilha',
        'constraint_type': 'equals',
        'value': codigo
      }]
    });
    
    if (produtos.length === 0) {
      return res.status(404).json({
        error: 'Produto não encontrado'
      });
    }
    
    const produto = produtos[0];
    console.log(`📦 Produto encontrado:`, produto);
    
    // Buscar relações do produto
    const relacoes = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco', {
      'constraints': [{
        'key': 'produto',
        'constraint_type': 'equals',
        'value': produto._id
      }]
    });
    
    console.log(`🔗 Relações encontradas: ${relacoes.length}`);
    
    // Buscar fornecedores das relações
    const fornecedorIds = [...new Set(relacoes.map(r => r.fornecedor))];
    console.log(`👥 IDs de fornecedores: [${fornecedorIds.join(', ')}]`);
    
    const fornecedoresPromises = fornecedorIds.map(async (id) => {
      const fornecedores = await fetchAllFromBubble('1 - fornecedor_25marco', {
        'constraints': [{
          'key': '_id',
          'constraint_type': 'equals',
          'value': id
        }]
      });
      return fornecedores[0];
    });
    
    const fornecedoresList = await Promise.all(fornecedoresPromises);
    const fornecedorMap = new Map();
    fornecedoresList.forEach(f => {
      if (f) fornecedorMap.set(f._id, f);
    });
    
    console.log(`👥 Fornecedores carregados: ${fornecedorMap.size}`);
    
    const relacoesDetalhadas = relacoes.map(r => {
      const fornecedor = fornecedorMap.get(r.fornecedor);
      return {
        fornecedor: fornecedor?.nome_fornecedor || 'Desconhecido',
        preco_original: r.preco_original,
        preco_final: r.preco_final,
        melhor_preco: r.melhor_preco,
        status_ativo: r.status_ativo,
        preco_ordenacao: r.preco_ordenacao
      };
    });
    
    // Recalcular estatísticas em tempo real para debugging
    const relacoesAtivas = relacoes.filter(r => r.status_ativo === 'yes' && r.preco_final > 0);
    const precosValidos = relacoesAtivas.map(r => r.preco_final);
    const statsCalculadas = {
      qtd_fornecedores: precosValidos.length,
      menor_preco: precosValidos.length > 0 ? Math.min(...precosValidos) : 0,
      preco_medio: precosValidos.length > 0 ? 
        Math.round((precosValidos.reduce((a, b) => a + b, 0) / precosValidos.length) * 100) / 100 : 0
    };
    
    console.log(`📊 Stats calculadas em tempo real:`, statsCalculadas);
    console.log(`📊 Stats salvas no banco:`, {
      qtd_fornecedores: produto.qtd_fornecedores,
      menor_preco: produto.menor_preco,
      preco_medio: produto.preco_medio
    });
    
    res.json({
      produto: {
        codigo: produto.id_planilha,
        nome: produto.nome_completo,
        preco_menor: produto.menor_preco,
        preco_medio: produto.preco_medio,
        qtd_fornecedores: produto.qtd_fornecedores
      },
      stats_calculadas_tempo_real: statsCalculadas,
      relacoes: relacoesDetalhadas.sort((a, b) => a.preco_final - b.preco_final),
      debug: {
        total_relacoes: relacoes.length,
        relacoes_ativas: relacoesAtivas.length,
        precos_validos: precosValidos,
        fornecedores_encontrados: fornecedorMap.size
      },
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Erro ao buscar produto:', error);
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
    version: '4.0.0-optimized',
    configuracoes: {
      batch_size: PROCESSING_CONFIG.BATCH_SIZE,
      max_concurrent: PROCESSING_CONFIG.MAX_CONCURRENT,
      retry_attempts: PROCESSING_CONFIG.RETRY_ATTEMPTS,
      request_timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT + 'ms'
    },
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
      bubble_response: {
        status: testResponse.status,
        count: testResponse.data?.response?.count || 0,
        remaining: testResponse.data?.response?.remaining || 0
      },
      config: {
        baseURL: BUBBLE_CONFIG.baseURL,
        hasToken: !!BUBBLE_CONFIG.token,
        timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
      },
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Erro de conectividade com Bubble',
      details: {
        message: error.message,
        status: error.response?.status,
        timeout: error.code === 'ECONNABORTED'
      },
      timestamp: new Date().toISOString()
    });
  }
});

// Rota para monitoramento de performance
app.get('/performance', (req, res) => {
  const memoryUsage = process.memoryUsage();
  const cpuUsage = process.cpuUsage();
  
  res.json({
    memoria: {
      rss: (memoryUsage.rss / 1024 / 1024).toFixed(2) + ' MB',
      heapTotal: (memoryUsage.heapTotal / 1024 / 1024).toFixed(2) + ' MB',
      heapUsed: (memoryUsage.heapUsed / 1024 / 1024).toFixed(2) + ' MB',
      external: (memoryUsage.external / 1024 / 1024).toFixed(2) + ' MB'
    },
    cpu: {
      user: cpuUsage.user,
      system: cpuUsage.system
    },
    uptime: Math.floor(process.uptime()) + ' segundos',
    configuracoes_otimizacao: PROCESSING_CONFIG,
    timestamp: new Date().toISOString()
  });
});

// Rota de documentação
app.get('/', (req, res) => {
  res.json({
    message: 'API OTIMIZADA para processamento de CSV de produtos com integração Bubble',
    version: '4.0.0-optimized',
    melhorias: [
      'Processamento em lotes (batch processing)',
      'Controle de concorrência',
      'Sistema de retry automático',
      'Otimização de memória',
      'Timeouts configuráveis',
      'Monitoramento de performance',
      'Tratamento robusto de erros',
      'Preparação de operações em memória',
      'Garbage collection otimizado'
    ],
    endpoints: {
      'POST /process-csv': 'Envia arquivo CSV com parâmetro gordura_valor e sincroniza com Bubble',
      'GET /stats': 'Retorna estatísticas das tabelas',
      'GET /produto/:codigo': 'Busca produto específico por código',
      'GET /health': 'Verifica status da API',
      'GET /test-bubble': 'Testa conectividade com Bubble',
      'GET /performance': 'Monitora performance do servidor'
    },
    parametros_obrigatorios: {
      'gordura_valor': 'number - Valor a ser adicionado ao preço original'
    },
    configuracoes_performance: {
      'tamanho_lote': PROCESSING_CONFIG.BATCH_SIZE + ' itens',
      'max_concorrencia': PROCESSING_CONFIG.MAX_CONCURRENT + ' operações simultâneas',
      'tentativas_retry': PROCESSING_CONFIG.RETRY_ATTEMPTS,
      'timeout_requisicao': PROCESSING_CONFIG.REQUEST_TIMEOUT + 'ms',
      'limite_arquivo': '100MB'
    },
    funcionalidades: [
      'Processamento de CSV com layout horizontal',
      'Cotação diária completa (zera produtos não cotados)',
      'Cálculos baseados no preço final (com margem)',
      'Identificação automática do melhor preço',
      'Sincronização inteligente com Bubble',
      'Processamento otimizado para alto volume',
      'Monitoramento de erros e performance'
    ]
  });
});

// Middleware de tratamento de erros otimizado
app.use((error, req, res, next) => {
  console.error('🚨 Erro capturado pelo middleware:', error);
  
  if (error instanceof multer.MulterError) {
    if (error.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ 
        error: 'Arquivo muito grande (máximo 100MB)',
        codigo: 'FILE_TOO_LARGE'
      });
    }
    if (error.code === 'LIMIT_UNEXPECTED_FILE') {
      return res.status(400).json({ 
        error: 'Campo de arquivo inesperado',
        codigo: 'UNEXPECTED_FILE'
      });
    }
  }
  
  if (error.message === 'Apenas arquivos CSV são permitidos!') {
    return res.status(400).json({ 
      error: 'Apenas arquivos CSV são permitidos',
      codigo: 'INVALID_FILE_TYPE'
    });
  }
  
  // Erro de timeout
  if (error.code === 'ECONNABORTED') {
    return res.status(408).json({
      error: 'Timeout na requisição',
      codigo: 'REQUEST_TIMEOUT',
      details: 'A operação demorou mais que o esperado'
    });
  }
  
  // Erro de conexão
  if (error.code === 'ECONNREFUSED') {
    return res.status(503).json({
      error: 'Serviço indisponível',
      codigo: 'SERVICE_UNAVAILABLE',
      details: 'Não foi possível conectar ao serviço externo'
    });
  }
  
  console.error('Erro não tratado:', error);
  res.status(500).json({ 
    error: 'Erro interno do servidor',
    codigo: 'INTERNAL_ERROR',
    timestamp: new Date().toISOString()
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('🛑 Recebido SIGTERM, encerrando servidor graciosamente...');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('🛑 Recebido SIGINT, encerrando servidor graciosamente...');
  process.exit(0);
});

// Tratamento de exceções não capturadas
process.on('uncaughtException', (error) => {
  console.error('🚨 Exceção não capturada:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('🚨 Promise rejection não tratada:', reason);
  console.error('Promise:', promise);
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log(`🚀 Servidor OTIMIZADO rodando na porta ${PORT}`);
  console.log(`📊 Acesse: http://localhost:${PORT}`);
  console.log(`🔗 Integração Bubble configurada`);
  console.log(`⚡ Versão 4.0.0-optimized - Otimizado para alto volume`);
  console.log(`📈 Configurações de performance:`);
  console.log(`   - Lote: ${PROCESSING_CONFIG.BATCH_SIZE} itens`);
  console.log(`   - Concorrência: ${PROCESSING_CONFIG.MAX_CONCURRENT} operações`);
  console.log(`   - Retry: ${PROCESSING_CONFIG.RETRY_ATTEMPTS} tentativas`);
  console.log(`   - Timeout: ${PROCESSING_CONFIG.REQUEST_TIMEOUT}ms`);
  console.log(`   - Limite arquivo: 100MB`);
});

module.exports = app;