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

// NOVA FUNÇÃO: Verificar se código é válido
function isCodigoValido(codigo) {
  if (!codigo || codigo.toString().trim() === '' || codigo.toString().trim().toUpperCase() === 'SEM CÓDIGO') {
    return false;
  }
  return true;
}

// NOVA FUNÇÃO: Gerar identificador único para produto
function gerarIdentificadorProduto(codigo, modelo) {
  const codigoLimpo = codigo ? codigo.toString().trim() : '';
  const modeloLimpo = modelo ? modelo.toString().trim() : '';
  
  // Se código é válido, usar código
  if (isCodigoValido(codigoLimpo)) {
    return {
      identificador: codigoLimpo,
      tipo: 'codigo',
      id_planilha: codigoLimpo,
      nome_completo: modeloLimpo
    };
  }
  
  // Se código não é válido, usar modelo como identificador
  if (modeloLimpo && modeloLimpo !== '') {
    return {
      identificador: modeloLimpo,
      tipo: 'modelo',
      id_planilha: '', // Deixar vazio quando não há código válido
      nome_completo: modeloLimpo
    };
  }
  
  // Se nem código nem modelo são válidos, retornar null
  return null;
}

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

// FUNÇÃO FINAL CORRETA - COM PROCESSAMENTO EM LOTES PARA ALTA VELOCIDADE (CORRIGIDA)
async function executarLogicaFinalCorreta() {
  console.log('\n🔥 === EXECUTANDO LÓGICA FINAL CORRETA (ÚLTIMA COISA) ===');
  
  try {
    // 1. Buscar TODOS os itens da tabela "1 - ProdutoFornecedor_25marco" COM PAGINAÇÃO CORRETA
    console.log('📊 1. Buscando TODOS os itens da tabela "1 - ProdutoFornecedor_25marco"...');
    
    let todosOsItens = [];
    let cursor = 0;
    let remaining = 1; // Iniciar com 1 para entrar no loop
    
    while (remaining > 0) {
      console.log(`📊 Buscando página com cursor: ${cursor}`);
      
      const response = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - ProdutoFornecedor _25marco`, {
        headers: BUBBLE_CONFIG.headers,
        params: { cursor, limit: 100 },
        timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
      });
      
      const data = response.data;
      
      if (!data.response || !data.response.results) {
        throw new Error('Resposta inválida da API');
      }
      
      todosOsItens = todosOsItens.concat(data.response.results);
      remaining = data.response.remaining || 0;
      
      console.log(`📊 Página carregada: ${data.response.results.length} itens (remaining: ${remaining})`);
      
      // INCREMENTAR CURSOR DE 100 EM 100
      cursor += 100;
      
      if (remaining > 0) {
        await delay(50); // Delay entre páginas
      }
    }
    
    console.log(`📊 Total de itens carregados: ${todosOsItens.length}`);
    
    // 2. Agrupar pelo campo "produto" DESDE QUE preco_final não seja 0 nem vazio
    console.log('📊 2. Agrupando pelo campo "produto"...');
    const grupos = {};
    
    todosOsItens.forEach(item => {
      // DESDE QUE preco_final não seja 0 nem vazio
      if (item.preco_final && item.preco_final > 0) {
        const produtoId = item.produto; // Campo "produto" = _id da tabela produtos
        
        if (!grupos[produtoId]) {
          grupos[produtoId] = [];
        }
        grupos[produtoId].push(item);
      }
    });
    
    const produtoIds = Object.keys(grupos);
    console.log(`📊 Produtos agrupados: ${produtoIds.length}`);
    
    // 3. PREPARAR OPERAÇÕES EM LOTES PARA ALTA VELOCIDADE
    console.log('📊 3. Preparando operações em lotes...');
    const operacoesProdutos = [];
    const operacoesMelhorPreco = [];
    
    for (const produtoId of produtoIds) {
      const grupo = grupos[produtoId];
      
      // Extrair preco_final de todos os itens do grupo
      const precosFinal = grupo.map(item => item.preco_final);
      
      // CALCULAR conforme especificado:
      const qtd_fornecedores = grupo.length;
      const menor_preco = Math.min(...precosFinal);
      const soma = precosFinal.reduce((a, b) => a + b, 0);
      const preco_medio = Math.round((soma / qtd_fornecedores) * 100) / 100;
      const itemComMenorPreco = grupo.find(item => item.preco_final === menor_preco);
      const fornecedor_menor_preco = itemComMenorPreco.fornecedor;
      
      // PREPARAR operação para produto
      operacoesProdutos.push({
        produtoId: produtoId,
        dados: {
          qtd_fornecedores: qtd_fornecedores,
          menor_preco: menor_preco,
          preco_medio: preco_medio,
          fornecedor_menor_preco: fornecedor_menor_preco
        },
        debug: {
          grupo_size: grupo.length,
          precos: precosFinal
        }
      });
      
      // PREPARAR operações para melhor_preco
      grupo.forEach(item => {
        const melhor_preco = (item.preco_final === menor_preco) ? 'yes' : 'no';
        
        operacoesMelhorPreco.push({
          itemId: item._id,
          melhor_preco: melhor_preco,
          debug: {
            preco_item: item.preco_final,
            menor_preco_grupo: menor_preco
          }
        });
      });
    }
    
    console.log(`📊 Operações preparadas:`);
    console.log(`   Produtos para editar: ${operacoesProdutos.length}`);
    console.log(`   Itens melhor_preco para editar: ${operacoesMelhorPreco.length}`);
    
    // 4. EXECUTAR OPERAÇÕES DOS PRODUTOS EM LOTES
    console.log('\n📦 4. Editando produtos em lotes...');
    const { results: produtoResults, errors: produtoErrors } = await processBatch(
      operacoesProdutos,
      async (operacao) => {
        console.log(`📦 Editando produto ${operacao.produtoId}: qtd=${operacao.dados.qtd_fornecedores}, menor=${operacao.dados.menor_preco}, media=${operacao.dados.preco_medio}`);
        
        return await updateInBubble('1 - produtos_25marco', operacao.produtoId, operacao.dados);
      }
    );
    
    const produtosEditados = produtoResults.filter(r => r.success).length;
    console.log(`✅ Produtos editados: ${produtosEditados}/${operacoesProdutos.length}`);
    
    // 5. EXECUTAR OPERAÇÕES DE MELHOR_PRECO EM LOTES
    console.log('\n🏷️ 5. Editando melhor_preco em lotes...');
    const { results: melhorPrecoResults, errors: melhorPrecoErrors } = await processBatch(
      operacoesMelhorPreco,
      async (operacao) => {
        console.log(`🏷️ Item ${operacao.itemId}: melhor_preco=${operacao.melhor_preco} (${operacao.debug.preco_item} vs ${operacao.debug.menor_preco_grupo})`);
        
        return await updateInBubble('1 - ProdutoFornecedor _25marco', operacao.itemId, {
          melhor_preco: operacao.melhor_preco
        });
      }
    );
    
    const itensEditados = melhorPrecoResults.filter(r => r.success).length;
    console.log(`✅ Itens melhor_preco editados: ${itensEditados}/${operacoesMelhorPreco.length}`);
    
    // 6. GARANTIR QUE ITENS INVÁLIDOS TENHAM MELHOR_PRECO = NO (EM LOTES)
    console.log('\n🧹 6. Garantindo melhor_preco=no para preços inválidos (em lotes)...');
    const itensInvalidos = todosOsItens.filter(item => !item.preco_final || item.preco_final <= 0);
    
    let itensInvalidosEditados = 0;
    
    if (itensInvalidos.length > 0) {
      console.log(`🧹 Encontrados ${itensInvalidos.length} itens com preços inválidos`);
      
      const operacoesInvalidos = itensInvalidos.map(item => ({
        itemId: item._id,
        preco_invalido: item.preco_final
      }));
      
      const { results: invalidosResults, errors: invalidosErrors } = await processBatch(
        operacoesInvalidos,
        async (operacao) => {
          console.log(`🧹 Item ${operacao.itemId}: melhor_preco=no (preço inválido: ${operacao.preco_invalido})`);
          
          return await updateInBubble('1 - ProdutoFornecedor _25marco', operacao.itemId, {
            melhor_preco: 'no'
          });
        }
      );
      
      itensInvalidosEditados = invalidosResults.filter(r => r.success).length;
      console.log(`✅ Itens inválidos editados: ${itensInvalidosEditados}/${itensInvalidos.length}`);
    }
    
    const resultados = {
      total_itens_carregados: todosOsItens.length,
      produtos_agrupados: produtoIds.length,
      produtos_editados: produtosEditados,
      itens_editados: itensEditados,
      itens_invalidos: itensInvalidos.length,
      itens_invalidos_editados: itensInvalidosEditados,
      erros: {
        produtos: produtoErrors.length,
        melhor_preco: melhorPrecoErrors.length
      },
      sucesso: true
    };
    
    console.log('\n🔥 === LÓGICA FINAL CORRETA CONCLUÍDA (COM LOTES) ===');
    console.log('📊 RESULTADOS:', resultados);
    
    return resultados;
    
  } catch (error) {
    console.error('❌ ERRO na lógica final correta:', error);
    throw error;
  }
}

// Função otimizada para processar o CSV (CORRIGIDA)
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
        let produtosSemCodigo = 0;
        let produtosComCodigo = 0;
        
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
            
            if (modelo && preco && 
                modelo.trim() !== '' && 
                preco.trim() !== '') {
              
              const precoNumerico = extractPrice(preco);
              
              // NOVA LÓGICA: Gerar identificador usando código ou modelo
              const produtoInfo = gerarIdentificadorProduto(codigo, modelo);
              
              if (produtoInfo) {
                produtos.push({
                  codigo: codigo ? codigo.trim() : '',
                  modelo: modelo.trim(),
                  preco: precoNumerico,
                  identificador: produtoInfo.identificador,
                  tipo_identificador: produtoInfo.tipo,
                  id_planilha: produtoInfo.id_planilha,
                  nome_completo: produtoInfo.nome_completo
                });
                
                if (produtoInfo.tipo === 'codigo') {
                  produtosComCodigo++;
                } else {
                  produtosSemCodigo++;
                }
              }
            }
          });
          
          // Forçar garbage collection a cada chunk
          if (global.gc && i % (chunkSize * 5) === 0) {
            global.gc();
          }
        }
        
        console.log(`✅ ${lojaConfig.nome}: ${produtos.length} produtos (${produtosComCodigo} com código válido, ${produtosSemCodigo} sem código)`);
        
        if (produtos.length > 0) {
          processedData.push({
            loja: lojaConfig.nome,
            total_produtos: produtos.length,
            produtos_com_codigo: produtosComCodigo,
            produtos_sem_codigo: produtosSemCodigo,
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

// Função principal para sincronizar com o Bubble - OTIMIZADA E CORRIGIDA
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
    
    // 2. CRIAR MAPAS OTIMIZADOS PARA BUSCA RÁPIDA (CORRIGIDOS)
    const fornecedorMap = new Map();
    fornecedores.forEach(f => fornecedorMap.set(f.nome_fornecedor, f));
    
    // MAPAS DUPLOS: Por id_planilha E por nome_completo
    const produtoMapPorCodigo = new Map();
    const produtoMapPorNome = new Map();
    
    produtos.forEach(p => {
      // Mapa por código (id_planilha) - apenas se não estiver vazio
      if (p.id_planilha && p.id_planilha.trim() !== '') {
        produtoMapPorCodigo.set(p.id_planilha, p);
      }
      
      // Mapa por nome (nome_completo)
      if (p.nome_completo && p.nome_completo.trim() !== '') {
        produtoMapPorNome.set(p.nome_completo, p);
      }
    });
    
    const relacaoMap = new Map();
    produtoFornecedores.forEach(pf => {
      relacaoMap.set(`${pf.produto}-${pf.fornecedor}`, pf);
    });
    
    console.log(`📊 Mapas criados: ${produtoMapPorCodigo.size} produtos por código, ${produtoMapPorNome.size} produtos por nome`);
    
    const results = {
      fornecedores_criados: 0,
      produtos_criados: 0,
      produtos_com_codigo: 0,
      produtos_sem_codigo: 0,
      relacoes_criadas: 0,
      relacoes_atualizadas: 0,
      relacoes_zeradas: 0,
      erros: []
    };
    
    // FUNÇÃO AUXILIAR: Buscar produto existente usando identificador correto
    function buscarProdutoExistente(produtoCsv) {
      if (produtoCsv.tipo_identificador === 'codigo') {
        return produtoMapPorCodigo.get(produtoCsv.identificador);
      } else {
        return produtoMapPorNome.get(produtoCsv.identificador);
      }
    }
    
    // 3. PREPARAR TODAS AS OPERAÇÕES EM MEMÓRIA PRIMEIRO - COM VERIFICAÇÃO DE DUPLICATAS (CORRIGIDA)
    console.log('\n📝 Preparando operações...');
    const operacoesFornecedores = [];
    const operacoesProdutos = [];
    const operacoesRelacoes = [];
    
    // Sets para evitar duplicatas nas operações - USANDO IDENTIFICADOR CORRETO
    const fornecedoresParaCriar = new Set();
    const produtosParaCriar = new Set(); // Usar identificador único
    
    // Coletar todos os identificadores cotados por fornecedor para lógica de cotação diária
    const identificadoresCotadosPorFornecedor = new Map();
    
    for (const lojaData of csvData) {
      const identificadoresCotados = new Set();
      
      // 3.1 Verificar fornecedor (evitar duplicatas)
      if (!fornecedorMap.has(lojaData.loja) && !fornecedoresParaCriar.has(lojaData.loja)) {
        fornecedoresParaCriar.add(lojaData.loja);
        operacoesFornecedores.push({
          tipo: 'criar',
          nome: lojaData.loja,
          dados: {
            nome_fornecedor: lojaData.loja
          }
        });
      }
      
      // 3.2 Processar produtos da loja (evitar duplicatas) - NOVA LÓGICA
      for (const produtoCsv of lojaData.produtos) {
        identificadoresCotados.add(produtoCsv.identificador);
        
        // Verificar se produto já existe usando identificador correto
        const produtoExistente = buscarProdutoExistente(produtoCsv);
        
        // Se produto não existe e não está na lista para criar
        if (!produtoExistente && !produtosParaCriar.has(produtoCsv.identificador)) {
          produtosParaCriar.add(produtoCsv.identificador);
          
          operacoesProdutos.push({
            tipo: 'criar',
            identificador: produtoCsv.identificador,
            tipo_identificador: produtoCsv.tipo_identificador,
            dados: {
              id_planilha: produtoCsv.id_planilha,
              nome_completo: produtoCsv.nome_completo,
              preco_medio: 0,
              qtd_fornecedores: 0,
              menor_preco: 0
            }
          });
          
          // Atualizar mapas locais para evitar duplicatas
          if (produtoCsv.tipo_identificador === 'codigo' && produtoCsv.id_planilha) {
            produtoMapPorCodigo.set(produtoCsv.identificador, {
              _id: 'temp_' + produtoCsv.identificador,
              id_planilha: produtoCsv.id_planilha,
              nome_completo: produtoCsv.nome_completo
            });
          } else {
            produtoMapPorNome.set(produtoCsv.identificador, {
              _id: 'temp_' + produtoCsv.identificador,
              id_planilha: produtoCsv.id_planilha,
              nome_completo: produtoCsv.nome_completo
            });
          }
          
          if (produtoCsv.tipo_identificador === 'codigo') {
            results.produtos_com_codigo++;
          } else {
            results.produtos_sem_codigo++;
          }
        }
        
        // Calcular preços
        const precoOriginal = produtoCsv.preco;
        const precoFinal = precoOriginal === 0 ? 0 : precoOriginal + gorduraValor;
        const precoOrdenacao = precoOriginal === 0 ? 999999 : precoOriginal;
        
        // Preparar operação de relação
        operacoesRelacoes.push({
          tipo: 'processar',
          loja: lojaData.loja,
          identificador: produtoCsv.identificador,
          tipo_identificador: produtoCsv.tipo_identificador,
          codigo: produtoCsv.codigo,
          modelo: produtoCsv.modelo,
          precoOriginal,
          precoFinal,
          precoOrdenacao
        });
      }
      
      identificadoresCotadosPorFornecedor.set(lojaData.loja, identificadoresCotados);
    }
    
    console.log(`📋 Operações preparadas: ${operacoesFornecedores.length} fornecedores únicos, ${operacoesProdutos.length} produtos únicos (${results.produtos_com_codigo} com código, ${results.produtos_sem_codigo} sem código), ${operacoesRelacoes.length} relações`);
    
    // 4. EXECUTAR OPERAÇÕES EM LOTES
    
    // 4.1 Criar fornecedores em lotes - SEM CONSTRAINTS PROBLEMÁTICAS
    if (operacoesFornecedores.length > 0) {
      console.log('\n👥 Criando fornecedores...');
      
      // Buscar TODOS os fornecedores existentes de uma vez
      const fornecedoresExistentes = await fetchAllFromBubble('1 - fornecedor_25marco');
      const fornecedoresExistentesMap = new Map();
      fornecedoresExistentes.forEach(f => fornecedoresExistentesMap.set(f.nome_fornecedor, f));
      
      const fornecedoresParaCriar = [];
      
      for (const operacao of operacoesFornecedores) {
        // Verificação no mapa de fornecedores existentes
        if (!fornecedorMap.has(operacao.nome) && !fornecedoresExistentesMap.has(operacao.nome)) {
          fornecedoresParaCriar.push(operacao);
        } else if (fornecedoresExistentesMap.has(operacao.nome)) {
          // Fornecedor já existe, adicionar ao mapa local
          const fornecedor = fornecedoresExistentesMap.get(operacao.nome);
          fornecedorMap.set(operacao.nome, {
            _id: fornecedor._id,
            nome_fornecedor: fornecedor.nome_fornecedor
          });
          console.log(`📋 Fornecedor ${operacao.nome} já existe, pulando criação`);
        }
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
    
    // 4.2 Criar produtos em lotes - SEM CONSTRAINTS PROBLEMÁTICAS (CORRIGIDA)
    if (operacoesProdutos.length > 0) {
      console.log('\n📦 Criando produtos...');
      
      // Buscar TODOS os produtos existentes de uma vez
      const produtosExistentes = await fetchAllFromBubble('1 - produtos_25marco');
      const produtosExistentesPorCodigo = new Map();
      const produtosExistentesPorNome = new Map();
      
      produtosExistentes.forEach(p => {
        if (p.id_planilha && p.id_planilha.trim() !== '') {
          produtosExistentesPorCodigo.set(p.id_planilha, p);
        }
        if (p.nome_completo && p.nome_completo.trim() !== '') {
          produtosExistentesPorNome.set(p.nome_completo, p);
        }
      });
      
      const produtosParaCriar = [];
      
      for (const operacao of operacoesProdutos) {
        let produtoExistente = null;
        
        // Verificar existência baseado no tipo de identificador
        if (operacao.tipo_identificador === 'codigo') {
          produtoExistente = produtoMapPorCodigo.get(operacao.identificador) || 
                            produtosExistentesPorCodigo.get(operacao.identificador);
        } else {
          produtoExistente = produtoMapPorNome.get(operacao.identificador) || 
                            produtosExistentesPorNome.get(operacao.identificador);
        }
        
        if (!produtoExistente || produtoExistente._id?.startsWith('temp_')) {
          produtosParaCriar.push(operacao);
        } else {
          // Produto já existe, adicionar aos mapas locais
          if (operacao.tipo_identificador === 'codigo' && operacao.dados.id_planilha) {
            produtoMapPorCodigo.set(operacao.identificador, {
              _id: produtoExistente._id,
              id_planilha: produtoExistente.id_planilha,
              nome_completo: produtoExistente.nome_completo
            });
          } else {
            produtoMapPorNome.set(operacao.identificador, {
              _id: produtoExistente._id,
              id_planilha: produtoExistente.id_planilha,
              nome_completo: produtoExistente.nome_completo
            });
          }
          console.log(`📋 Produto ${operacao.identificador} (${operacao.tipo_identificador}) já existe, pulando criação`);
        }
      }
      
      console.log(`📦 Produtos únicos para criar: ${produtosParaCriar.length} de ${operacoesProdutos.length}`);
      
      if (produtosParaCriar.length > 0) {
        const { results: produtoResults, errors: produtoErrors } = await processBatch(
          produtosParaCriar,
          async (operacao) => {
            // Verificação final antes de criar
            let produtoExistente = null;
            if (operacao.tipo_identificador === 'codigo') {
              produtoExistente = produtoMapPorCodigo.get(operacao.identificador);
            } else {
              produtoExistente = produtoMapPorNome.get(operacao.identificador);
            }
            
            if (produtoExistente && !produtoExistente._id?.startsWith('temp_')) {
              return { skipped: true, identificador: operacao.identificador };
            }
            
            const novoProduto = await createInBubble('1 - produtos_25marco', operacao.dados);
            
            // Atualizar mapas locais com o produto criado
            const produtoCompleto = {
              _id: novoProduto.id,
              id_planilha: operacao.dados.id_planilha,
              nome_completo: operacao.dados.nome_completo
            };
            
            if (operacao.tipo_identificador === 'codigo' && operacao.dados.id_planilha) {
              produtoMapPorCodigo.set(operacao.identificador, produtoCompleto);
            } else {
              produtoMapPorNome.set(operacao.identificador, produtoCompleto);
            }
            
            return novoProduto;
          }
        );
        results.produtos_criados = produtoResults.filter(r => r.success && !r.result?.skipped).length;
        results.erros.push(...produtoErrors);
      }
    }
    
    // 4.3 Processar relações em lotes (CORRIGIDA)
    console.log('\n🔗 Processando relações...');
    const { results: relacaoResults, errors: relacaoErrors } = await processBatch(
      operacoesRelacoes,
      async (operacao) => {
        const fornecedor = fornecedorMap.get(operacao.loja);
        
        // Buscar produto usando identificador correto
        let produto = null;
        if (operacao.tipo_identificador === 'codigo') {
          produto = produtoMapPorCodigo.get(operacao.identificador);
        } else {
          produto = produtoMapPorNome.get(operacao.identificador);
        }
        
        if (!fornecedor || !produto) {
          throw new Error(`Fornecedor ou produto não encontrado: ${operacao.loja} - ${operacao.identificador} (${operacao.tipo_identificador})`);
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
            melhor_preco: false
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
    
    // 4.4 APLICAR LÓGICA DE COTAÇÃO DIÁRIA (CORRIGIDA)
    console.log('\n🧹 Aplicando lógica de cotação diária...');
    const operacoesZeramento = [];
    
    for (const [lojaName, identificadoresCotadosHoje] of identificadoresCotadosPorFornecedor) {
      const fornecedor = fornecedorMap.get(lojaName);
      if (!fornecedor) continue;
      
      const relacoesExistentes = produtoFornecedores.filter(pf => pf.fornecedor === fornecedor._id);
      
      for (const relacao of relacoesExistentes) {
        const produto = produtos.find(p => p._id === relacao.produto);
        if (!produto) continue;
        
        // Verificar se foi cotado hoje usando AMBOS os identificadores
        let foiCotadoHoje = false;
        
        // Verificar por código (id_planilha)
        if (produto.id_planilha && produto.id_planilha.trim() !== '') {
          foiCotadoHoje = identificadoresCotadosHoje.has(produto.id_planilha);
        }
        
        // Se não foi cotado por código, verificar por nome
        if (!foiCotadoHoje && produto.nome_completo && produto.nome_completo.trim() !== '') {
          foiCotadoHoje = identificadoresCotadosHoje.has(produto.nome_completo);
        }
        
        const temPreco = relacao.preco_original > 0;
        
        if (!foiCotadoHoje && temPreco) {
          operacoesZeramento.push({
            relacaoId: relacao._id,
            identificador: produto.id_planilha || produto.nome_completo,
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
    
    console.log('\n✅ Sincronização otimizada concluída!');
    console.log('📊 Resultados da sincronização:', results);
    
    // === ESTA É A ÚLTIMA COISA QUE O CÓDIGO FAZ ===
    // === EXECUTAR A LÓGICA FINAL CORRETA ===
    console.log('\n🔥 EXECUTANDO LÓGICA FINAL CORRETA - ÚLTIMA COISA DO CÓDIGO!');
    const logicaFinalResults = await executarLogicaFinalCorreta();
    console.log('🔥 Lógica final correta concluída:', logicaFinalResults);
    
    return {
      ...results,
      logica_final_correta: logicaFinalResults
    };
    
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
        total_produtos: loja.total_produtos,
        produtos_com_codigo: loja.produtos_com_codigo,
        produtos_sem_codigo: loja.produtos_sem_codigo
      })),
      resultados_sincronizacao: syncResults,
      estatisticas_processamento: {
        total_lojas_processadas: csvData.length,
        total_produtos_csv: csvData.reduce((acc, loja) => acc + loja.total_produtos, 0),
        total_produtos_com_codigo: csvData.reduce((acc, loja) => acc + loja.produtos_com_codigo, 0),
        total_produtos_sem_codigo: csvData.reduce((acc, loja) => acc + loja.produtos_sem_codigo, 0),
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

// Rota para EXECUTAR LÓGICA FINAL CORRETA
app.post('/force-recalculate', async (req, res) => {
  try {
    console.log('\n🔥 === EXECUTANDO LÓGICA FINAL CORRETA MANUALMENTE ===');
    
    const startTime = Date.now();
    const results = await executarLogicaFinalCorreta();
    const endTime = Date.now();
    const processingTime = (endTime - startTime) / 1000;
    
    console.log(`🔥 Lógica final correta executada em ${processingTime}s`);
    
    res.json({
      success: true,
      message: 'LÓGICA FINAL CORRETA executada com sucesso',
      tempo_processamento: processingTime + 's',
      resultados: results,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Erro na lógica final correta:', error);
    res.status(500).json({
      error: 'Erro na LÓGICA FINAL CORRETA',
      details: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

app.get('/stats', async (req, res) => {
  try {
    const [fornecedores, produtos, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - fornecedor_25marco'),
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    // Estatísticas adicionais para produtos com/sem código
    const produtosComCodigo = produtos.filter(p => p.id_planilha && p.id_planilha.trim() !== '').length;
    const produtosSemCodigo = produtos.filter(p => !p.id_planilha || p.id_planilha.trim() === '').length;
    
    res.json({
      total_fornecedores: fornecedores.length,
      total_produtos: produtos.length,
      produtos_com_codigo: produtosComCodigo,
      produtos_sem_codigo: produtosSemCodigo,
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

// Rota para buscar produto específico - CORRIGIDA PARA IDENTIFICADOR DUPLO
app.get('/produto/:identificador', async (req, res) => {
  try {
    const identificador = req.params.identificador;
    console.log(`🔍 Buscando produto: ${identificador}`);
    
    // Buscar TODOS os produtos e filtrar localmente por AMBOS os campos
    const todosProdutos = await fetchAllFromBubble('1 - produtos_25marco');
    const produto = todosProdutos.find(p => 
      p.id_planilha === identificador || p.nome_completo === identificador
    );
    
    if (!produto) {
      return res.status(404).json({
        error: 'Produto não encontrado',
        message: `Nenhum produto encontrado com código ou nome: ${identificador}`
      });
    }
    
    console.log(`📦 Produto encontrado:`, produto);
    
    // Determinar o tipo de busca realizada
    const tipoBusca = produto.id_planilha === identificador ? 'codigo' : 'nome';
    
    // Buscar TODAS as relações e filtrar localmente
    const todasRelacoes = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco');
    const relacoes = todasRelacoes.filter(r => r.produto === produto._id);
    
    console.log(`🔗 Relações encontradas: ${relacoes.length}`);
    
    // Buscar TODOS os fornecedores e filtrar localmente
    const todosFornecedores = await fetchAllFromBubble('1 - fornecedor_25marco');
    const fornecedorMap = new Map();
    todosFornecedores.forEach(f => fornecedorMap.set(f._id, f));
    
    console.log(`👥 Fornecedores carregados: ${fornecedorMap.size}`);
    
    const relacoesDetalhadas = relacoes.map(r => {
      const fornecedor = fornecedorMap.get(r.fornecedor);
      return {
        fornecedor: fornecedor?.nome_fornecedor || 'Desconhecido',
        preco_original: r.preco_original,
        preco_final: r.preco_final,
        melhor_preco: r.melhor_preco,
        preco_ordenacao: r.preco_ordenacao
      };
    });
    
    // Recalcular estatísticas em tempo real para debugging - SEM STATUS_ATIVO
    const relacoesAtivas = relacoes.filter(r => r.preco_final > 0);
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
        codigo: produto.id_planilha || '',
        nome: produto.nome_completo,
        preco_menor: produto.menor_preco,
        preco_medio: produto.preco_medio,
        qtd_fornecedores: produto.qtd_fornecedores,
        tipo_identificador: produto.id_planilha && produto.id_planilha.trim() !== '' ? 'codigo' : 'nome'
      },
      busca_realizada: {
        identificador_buscado: identificador,
        tipo_busca: tipoBusca,
        encontrado_por: tipoBusca === 'codigo' ? 'id_planilha' : 'nome_completo'
      },
      stats_calculadas_tempo_real: statsCalculadas,
      relacoes: relacoesDetalhadas.sort((a, b) => a.preco_final - b.preco_final),
      debug: {
        total_relacoes: relacoes.length,
        relacoes_com_preco: relacoesAtivas.length,
        precos_validos: precosValidos,
        fornecedores_encontrados: fornecedorMap.size,
        produto_id_planilha: produto.id_planilha,
        produto_nome_completo: produto.nome_completo
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
    version: '4.1.0-codigo-corrigido',
    melhorias_versao: [
      'Tratamento correto de códigos vazios e "SEM CÓDIGO"',
      'Identificação dupla: por código (id_planilha) e por nome (nome_completo)',
      'Prevenção de duplicatas para produtos sem código válido',
      'Cotação diária com identificador correto',
      'API de busca por código OU nome'
    ],
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

// Nova rota para debug de produtos sem código
app.get('/debug/produtos-sem-codigo', async (req, res) => {
  try {
    console.log('🔍 Buscando produtos sem código válido...');
    
    const todosProdutos = await fetchAllFromBubble('1 - produtos_25marco');
    
    const produtosSemCodigo = todosProdutos.filter(p => 
      !p.id_planilha || p.id_planilha.trim() === '' || p.id_planilha.trim().toUpperCase() === 'SEM CÓDIGO'
    );
    
    const produtosComCodigo = todosProdutos.filter(p => 
      p.id_planilha && p.id_planilha.trim() !== '' && p.id_planilha.trim().toUpperCase() !== 'SEM CÓDIGO'
    );
    
    res.json({
      total_produtos: todosProdutos.length,
      produtos_com_codigo: produtosComCodigo.length,
      produtos_sem_codigo: produtosSemCodigo.length,
      porcentagem_sem_codigo: ((produtosSemCodigo.length / todosProdutos.length) * 100).toFixed(2) + '%',
      amostra_produtos_sem_codigo: produtosSemCodigo.slice(0, 10).map(p => ({
        _id: p._id,
        id_planilha: p.id_planilha || '(vazio)',
        nome_completo: p.nome_completo,
        qtd_fornecedores: p.qtd_fornecedores,
        menor_preco: p.menor_preco
      })),
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao buscar produtos sem código',
      details: error.message
    });
  }
});

// Rota de documentação
app.get('/', (req, res) => {
  res.json({
    message: 'API OTIMIZADA E CORRIGIDA para processamento de CSV de produtos com integração Bubble',
    version: '4.1.0-codigo-corrigido',
    correcoes_implementadas: [
      'Tratamento correto de códigos vazios e "SEM CÓDIGO"',
      'Uso de nome_completo como identificador quando código é inválido',
      'Prevenção de duplicatas para produtos sem código',
      'Mapas duplos para busca por código E por nome',
      'Lógica de cotação diária corrigida para ambos identificadores',
      'API de busca flexível (código OU nome)',
      'Debug específico para produtos sem código'
    ],
    melhorias_gerais: [
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
      'POST /force-recalculate': 'EXECUTA a lógica final de recálculo',
      'GET /stats': 'Retorna estatísticas das tabelas (incluindo produtos com/sem código)',
      'GET /produto/:identificador': 'Busca produto por código OU nome',
      'GET /debug/produtos-sem-codigo': 'Debug específico para produtos sem código válido',
      'GET /health': 'Verifica status da API',
      'GET /test-bubble': 'Testa conectividade com Bubble',
      'GET /performance': 'Monitora performance do servidor'
    },
    parametros_obrigatorios: {
      'gordura_valor': 'number - Valor a ser adicionado ao preço original'
    },
    tratamento_codigos: {
      'codigos_validos': 'Qualquer valor que não seja vazio nem "SEM CÓDIGO"',
      'codigos_invalidos': 'Vazio, null, undefined ou "SEM CÓDIGO"',
      'identificador_alternativo': 'nome_completo (modelo padronizado)',
      'busca_produto': 'Por id_planilha (código) OU nome_completo (nome)'
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
      'Tratamento inteligente de códigos inválidos',
      'Identificação dupla (código + nome)',
      'Cotação diária completa (zera produtos não cotados)',
      'Cálculos baseados no preço final (com margem)',
      'Identificação automática do melhor preço',
      'Sincronização inteligente com Bubble',
      'Processamento otimizado para alto volume',
      'Monitoramento de erros e performance',
      'Debug específico para casos especiais'
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
  console.log(`🚀 Servidor OTIMIZADO E CORRIGIDO rodando na porta ${PORT}`);
  console.log(`📊 Acesse: http://localhost:${PORT}`);
  console.log(`🔗 Integração Bubble configurada`);
  console.log(`⚡ Versão 4.1.0-codigo-corrigido - Tratamento correto de códigos inválidos`);
  console.log(`🔧 Correções implementadas:`);
  console.log(`   - Códigos vazios/"SEM CÓDIGO" usam nome_completo como identificador`);
  console.log(`   - Mapas duplos para busca rápida (código + nome)`);
  console.log(`   - Prevenção de duplicatas para produtos sem código`);
  console.log(`   - Cotação diária com identificador correto`);
  console.log(`   - API de busca flexível por código OU nome`);
  console.log(`📈 Configurações de performance:`);
  console.log(`   - Lote: ${PROCESSING_CONFIG.BATCH_SIZE} itens`);
  console.log(`   - Concorrência: ${PROCESSING_CONFIG.MAX_CONCURRENT} operações`);
  console.log(`   - Retry: ${PROCESSING_CONFIG.RETRY_ATTEMPTS} tentativas`);
  console.log(`   - Timeout: ${PROCESSING_CONFIG.REQUEST_TIMEOUT}ms`);
  console.log(`   - Limite arquivo: 100MB`);
});

module.exports = app;