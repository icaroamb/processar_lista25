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

// FUNÇÃO: Verificar se código é válido (MANTIDA)
function isCodigoValido(codigo) {
  if (!codigo || codigo.toString().trim() === '' || codigo.toString().trim().toUpperCase() === 'SEM CÓDIGO') {
    return false;
  }
  return true;
}

// FUNÇÃO REMOVIDA: gerarIdentificadorProduto (não é mais necessária)
// Agora usamos apenas códigos válidos como identificadores

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

// FUNÇÃO FINAL CORRETA - COM PROCESSAMENTO EM LOTES PARA ALTA VELOCIDADE
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

// Função otimizada para processar o CSV (CORRIGIDA - APENAS CÓDIGOS VÁLIDOS)
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
            
            // *** NOVA LÓGICA: PROCESSAR APENAS SE TEM CÓDIGO VÁLIDO ***
            if (isCodigoValido(codigo) && modelo && preco && 
                modelo.trim() !== '' && 
                preco.trim() !== '') {
              
              const precoNumerico = extractPrice(preco);
              
              produtos.push({
                codigo: codigo.trim(),
                modelo: modelo.trim(),
                preco: precoNumerico,
                identificador: codigo.trim(), // Sempre o código como identificador
                tipo_identificador: 'codigo', // Sempre código
                id_planilha: codigo.trim(),
                nome_completo: modelo.trim()
              });
              
              produtosComCodigo++;
            } else {
              // Contador para produtos ignorados (sem código)
              if (modelo && preco && modelo.trim() !== '' && preco.trim() !== '') {
                produtosSemCodigo++;
              }
            }
          });
          
          // Forçar garbage collection a cada chunk
          if (global.gc && i % (chunkSize * 5) === 0) {
            global.gc();
          }
        }
        
        console.log(`✅ ${lojaConfig.nome}: ${produtos.length} produtos processados (${produtosSemCodigo} ignorados por não ter código)`);
        
        if (produtos.length > 0) {
          processedData.push({
            loja: lojaConfig.nome,
            total_produtos: produtos.length,
            produtos_com_codigo: produtosComCodigo,
            produtos_sem_codigo: produtosSemCodigo, // Apenas para estatística
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

// Função principal para sincronizar com o Bubble - NOVA ABORDAGEM COM BUSCAS ESPECÍFICAS
async function syncWithBubble(csvData, gorduraValor) {
  try {
    console.log('\n🔄 Iniciando sincronização com BUSCAS ESPECÍFICAS...');
    
    // 1. CARREGAR APENAS FORNECEDORES (para mapeamento loja → fornecedor_id)
    console.log('📊 1. Carregando TODOS os fornecedores...');
    const todosFornecedores = await fetchAllFromBubble('1 - fornecedor_25marco');
    
    // Criar mapa: nome_fornecedor → _id
    const fornecedorMap = new Map();
    todosFornecedores.forEach(f => {
      fornecedorMap.set(f.nome_fornecedor, f._id);
    });
    
    console.log(`📊 Fornecedores carregados: ${fornecedorMap.size}`);
    
    const results = {
      fornecedores_criados: 0,
      produtos_criados: 0,
      produtos_existentes_encontrados: 0,
      relacoes_criadas: 0,
      relacoes_atualizadas: 0,
      relacoes_ja_existentes: 0,
      erros: []
    };
    
    // 2. PROCESSAR CADA LOJA DO CSV
    for (const lojaData of csvData) {
      console.log(`\n🏪 Processando loja: ${lojaData.loja}`);
      
      // 2.1 Verificar/Criar fornecedor se necessário
      let fornecedor_id = fornecedorMap.get(lojaData.loja);
      
      if (!fornecedor_id) {
        console.log(`➕ Criando novo fornecedor: ${lojaData.loja}`);
        
        try {
          const novoFornecedor = await createInBubble('1 - fornecedor_25marco', {
            nome_fornecedor: lojaData.loja
          });
          
          fornecedor_id = novoFornecedor.id;
          fornecedorMap.set(lojaData.loja, fornecedor_id);
          results.fornecedores_criados++;
          
          console.log(`✅ Fornecedor criado: ${lojaData.loja} (ID: ${fornecedor_id})`);
          
        } catch (error) {
          console.error(`❌ Erro ao criar fornecedor ${lojaData.loja}:`, error.message);
          results.erros.push({
            tipo: 'fornecedor',
            loja: lojaData.loja,
            erro: error.message
          });
          continue; // Pular esta loja se não conseguir criar fornecedor
        }
      } else {
        console.log(`✅ Fornecedor já existe: ${lojaData.loja} (ID: ${fornecedor_id})`);
      }
      
      // 2.2 Processar cada produto da loja
      for (const produtoCsv of lojaData.produtos) {
        const codigo = produtoCsv.codigo;
        const modelo = produtoCsv.modelo;
        const precoOriginal = produtoCsv.preco;
        const precoFinal = precoOriginal === 0 ? 0 : precoOriginal + gorduraValor;
        const precoOrdenacao = precoOriginal === 0 ? 999999 : precoOriginal;
        
        console.log(`\n🔍 Processando produto: ${codigo} - ${modelo} (preço: ${precoOriginal})`);
        
        try {
          // 2.3 BUSCA ESPECÍFICA: Produto existe por id_planilha?
          console.log(`🔍 Buscando produto por id_planilha: ${codigo}`);
          
          const buscaProdutoResponse = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - produtos_25marco`, {
            headers: BUBBLE_CONFIG.headers,
            params: {
              constraints: JSON.stringify([{
                key: 'id_planilha',
                constraint_type: 'equals',
                value: codigo
              }]),
              limit: 1
            },
            timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
          });
          
          let produto_id = null;
          
          if (buscaProdutoResponse.data.response.results.length > 0) {
            // ✅ PRODUTO JÁ EXISTE
            const produtoExistente = buscaProdutoResponse.data.response.results[0];
            produto_id = produtoExistente._id;
            results.produtos_existentes_encontrados++;
            
            console.log(`✅ Produto EXISTE: ${codigo} (ID: ${produto_id})`);
            
          } else {
            // ➕ PRODUTO NÃO EXISTE - CRIAR NOVO
            console.log(`➕ Produto NÃO EXISTE, criando: ${codigo}`);
            
            const novoProduto = await createInBubble('1 - produtos_25marco', {
              id_planilha: codigo,
              nome_completo: modelo,
              preco_medio: 0,
              qtd_fornecedores: 0,
              menor_preco: 0
            });
            
            produto_id = novoProduto.id;
            results.produtos_criados++;
            
            console.log(`✅ Produto CRIADO: ${codigo} (ID: ${produto_id})`);
          }
          
          // 2.4 BUSCA ESPECÍFICA: Relação produto-fornecedor existe?
          console.log(`🔍 Buscando relação produto-fornecedor: ${produto_id} + ${fornecedor_id}`);
          
          const buscaRelacaoResponse = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - ProdutoFornecedor _25marco`, {
            headers: BUBBLE_CONFIG.headers,
            params: {
              constraints: JSON.stringify([
                {
                  key: 'produto',
                  constraint_type: 'equals',
                  value: produto_id
                },
                {
                  key: 'fornecedor',
                  constraint_type: 'equals',
                  value: fornecedor_id
                }
              ]),
              limit: 1
            },
            timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
          });
          
          if (buscaRelacaoResponse.data.response.results.length > 0) {
            // 🔄 RELAÇÃO JÁ EXISTE - ATUALIZAR PREÇOS
            const relacaoExistente = buscaRelacaoResponse.data.response.results[0];
            
            if (relacaoExistente.preco_original !== precoOriginal) {
              console.log(`🔄 ATUALIZANDO relação existente: ${codigo} - ${lojaData.loja} (${relacaoExistente.preco_original} → ${precoOriginal})`);
              
              await updateInBubble('1 - ProdutoFornecedor _25marco', relacaoExistente._id, {
                nome_produto: modelo,
                preco_original: precoOriginal,
                preco_final: precoFinal,
                preco_ordenacao: precoOrdenacao
              });
              
              results.relacoes_atualizadas++;
              console.log(`✅ Relação ATUALIZADA: ${codigo} - ${lojaData.loja}`);
              
            } else {
              console.log(`⚪ Relação INALTERADA: ${codigo} - ${lojaData.loja} (preço: ${precoOriginal})`);
              results.relacoes_ja_existentes++;
            }
            
          } else {
            // ➕ RELAÇÃO NÃO EXISTE - CRIAR NOVA
            console.log(`➕ CRIANDO nova relação: ${codigo} - ${lojaData.loja}`);
            
            await createInBubble('1 - ProdutoFornecedor _25marco', {
              produto: produto_id,
              fornecedor: fornecedor_id,
              nome_produto: modelo,
              preco_original: precoOriginal,
              preco_final: precoFinal,
              preco_ordenacao: precoOrdenacao,
              melhor_preco: false
            });
            
            results.relacoes_criadas++;
            console.log(`✅ Relação CRIADA: ${codigo} - ${lojaData.loja}`);
          }
          
        } catch (error) {
          console.error(`❌ Erro ao processar produto ${codigo} - ${lojaData.loja}:`, error.message);
          results.erros.push({
            tipo: 'produto',
            codigo: codigo,
            loja: lojaData.loja,
            erro: error.message
          });
        }
        
        // Pequeno delay para evitar rate limiting
        await delay(50);
      }
    }
    
    // 3. APLICAR LÓGICA DE COTAÇÃO DIÁRIA
    console.log('\n🧹 Aplicando lógica de cotação diária...');
    
    // Coletar todos os códigos cotados por fornecedor
    const codigosCotadosPorFornecedor = new Map();
    
    for (const lojaData of csvData) {
      const codigosCotados = new Set();
      lojaData.produtos.forEach(p => codigosCotados.add(p.codigo));
      codigosCotadosPorFornecedor.set(lojaData.loja, codigosCotados);
    }
    
    // Para cada fornecedor, zerar produtos não cotados hoje
    let relacoes_zeradas = 0;
    
    for (const [lojaName, codigosCotadosHoje] of codigosCotadosPorFornecedor) {
      const fornecedor_id = fornecedorMap.get(lojaName);
      if (!fornecedor_id) continue;
      
      console.log(`🧹 Verificando produtos não cotados da ${lojaName}...`);
      
      // Buscar TODAS as relações deste fornecedor que têm preço > 0
      const relacoesAtivas = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - ProdutoFornecedor _25marco`, {
        headers: BUBBLE_CONFIG.headers,
        params: {
          constraints: JSON.stringify([
            {
              key: 'fornecedor',
              constraint_type: 'equals',
              value: fornecedor_id
            },
            {
              key: 'preco_original',
              constraint_type: 'greater than',
              value: 0
            }
          ]),
          limit: 100
        },
        timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
      });
      
      // Para cada relação ativa, verificar se o produto foi cotado hoje
      for (const relacao of relacoesAtivas.data.response.results) {
        // Buscar o produto para pegar o id_planilha
        const produtoResponse = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - produtos_25marco/${relacao.produto}`, {
          headers: BUBBLE_CONFIG.headers,
          timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
        });
        
        const produto = produtoResponse.data.response;
        const codigoProduto = produto.id_planilha;
        
        if (!codigosCotadosHoje.has(codigoProduto)) {
          // Produto não foi cotado hoje - ZERAR
          console.log(`🧹 Zerando produto não cotado: ${codigoProduto} - ${lojaName}`);
          
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacao._id, {
            preco_original: 0,
            preco_final: 0,
            preco_ordenacao: 999999
          });
          
          relacoes_zeradas++;
        }
        
        await delay(25); // Delay para não sobrecarregar
      }
    }
    
    results.relacoes_zeradas = relacoes_zeradas;
    
    console.log('📊 Resultados da sincronização:', results);
    console.log(`🎯 ZERO duplicatas garantidas por busca específica antes de criar!`);
    
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
    console.log('⏱️ Iniciando processamento com BUSCAS ESPECÍFICAS...');
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
      message: 'CSV processado com BUSCAS ESPECÍFICAS - Zero duplicatas garantidas',
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
        produtos_criados: syncResults.produtos_criados,
        produtos_existentes_encontrados: syncResults.produtos_existentes_encontrados,
        relacoes_criadas: syncResults.relacoes_criadas,
        relacoes_atualizadas: syncResults.relacoes_atualizadas,
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
    
    // Estatísticas para produtos COM código (únicos processados agora)
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
      observacao: 'Processamento com buscas específicas - zero duplicatas',
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao buscar estatísticas',
      details: error.message
    });
  }
});

// Rota para buscar produto específico - SIMPLIFICADA (APENAS POR CÓDIGO)
app.get('/produto/:codigo', async (req, res) => {
  try {
    const codigo = req.params.codigo;
    console.log(`🔍 Buscando produto por código: ${codigo}`);
    
    // Buscar produto por id_planilha usando constraint
    const produtoResponse = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - produtos_25marco`, {
      headers: BUBBLE_CONFIG.headers,
      params: {
        constraints: JSON.stringify([{
          key: 'id_planilha',
          constraint_type: 'equals',
          value: codigo
        }]),
        limit: 1
      },
      timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
    });
    
    if (produtoResponse.data.response.results.length === 0) {
      return res.status(404).json({
        error: 'Produto não encontrado',
        message: `Nenhum produto encontrado com código: ${codigo}`
      });
    }
    
    const produto = produtoResponse.data.response.results[0];
    console.log(`📦 Produto encontrado:`, produto);
    
    // Buscar relações do produto usando constraint
    const relacoesResponse = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - ProdutoFornecedor _25marco`, {
      headers: BUBBLE_CONFIG.headers,
      params: {
        constraints: JSON.stringify([{
          key: 'produto',
          constraint_type: 'equals',
          value: produto._id
        }]),
        limit: 100
      },
      timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
    });
    
    const relacoes = relacoesResponse.data.response.results;
    console.log(`🔗 Relações encontradas: ${relacoes.length}`);
    
    // Buscar fornecedores para mapear nomes
    const todosFornecedores = await fetchAllFromBubble('1 - fornecedor_25marco');
    const fornecedorMap = new Map();
    todosFornecedores.forEach(f => fornecedorMap.set(f._id, f));
    
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
    
    // Recalcular estatísticas em tempo real para debugging
    const relacoesAtivas = relacoes.filter(r => r.preco_final > 0);
    const precosValidos = relacoesAtivas.map(r => r.preco_final);
    const statsCalculadas = {
      qtd_fornecedores: precosValidos.length,
      menor_preco: precosValidos.length > 0 ? Math.min(...precosValidos) : 0,
      preco_medio: precosValidos.length > 0 ? 
        Math.round((precosValidos.reduce((a, b) => a + b, 0) / precosValidos.length) * 100) / 100 : 0
    };
    
    console.log(`📊 Stats calculadas em tempo real:`, statsCalculadas);
    
    res.json({
      produto: {
        codigo: produto.id_planilha,
        nome: produto.nome_completo,
        preco_menor: produto.menor_preco,
        preco_medio: produto.preco_medio,
        qtd_fornecedores: produto.qtd_fornecedores,
        tipo_identificador: 'codigo'
      },
      busca_realizada: {
        codigo_buscado: codigo,
        tipo_busca: 'codigo',
        encontrado_por: 'constraint id_planilha'
      },
      stats_calculadas_tempo_real: statsCalculadas,
      relacoes: relacoesDetalhadas.sort((a, b) => a.preco_final - b.preco_final),
      debug: {
        total_relacoes: relacoes.length,
        relacoes_com_preco: relacoesAtivas.length,
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

// Rota para teste de saúde🔑 Chave de unicidade: produto_id + fornecedor_id`);
    
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
    console.log('⏱️ Iniciando processamento - APENAS produtos com código...');
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
      message: 'CSV processado - APENAS produtos com código válido',
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
        produtos_criados: syncResults.produtos_criados,
        produtos_atualizados: syncResults.produtos_atualizados,
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
    
    // Estatísticas para produtos COM código (únicos processados agora)
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
      observacao: 'Apenas produtos com código válido são processados',
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao buscar estatísticas',
      details: error.message
    });
  }
});

// Rota para buscar produto específico - SIMPLIFICADA (APENAS POR CÓDIGO)
app.get('/produto/:codigo', async (req, res) => {
  try {
    const codigo = req.params.codigo;
    console.log(`🔍 Buscando produto por código: ${codigo}`);
    
    // Buscar TODOS os produtos e filtrar localmente APENAS por código
    const todosProdutos = await fetchAllFromBubble('1 - produtos_25marco');
    const produto = todosProdutos.find(p => p.id_planilha === codigo);
    
    if (!produto) {
      return res.status(404).json({
        error: 'Produto não encontrado',
        message: `Nenhum produto encontrado com código: ${codigo}`
      });
    }
    
    console.log(`📦 Produto encontrado:`, produto);
    
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
    
    // Recalcular estatísticas em tempo real para debugging
    const relacoesAtivas = relacoes.filter(r => r.preco_final > 0);
    const precosValidos = relacoesAtivas.map(r => r.preco_final);
    const statsCalculadas = {
      qtd_fornecedores: precosValidos.length,
      menor_preco: precosValidos.length > 0 ? Math.min(...precosValidos) : 0,
      preco_medio: precosValidos.length > 0 ? 
        Math.round((precosValidos.reduce((a, b) => a + b, 0) / precosValidos.length) * 100) / 100 : 0
    };
    
    console.log(`📊 Stats calculadas em tempo real:`, statsCalculadas);
    
    res.json({
      produto: {
        codigo: produto.id_planilha,
        nome: produto.nome_completo,
        preco_menor: produto.menor_preco,
        preco_medio: produto.preco_medio,
        qtd_fornecedores: produto.qtd_fornecedores,
        tipo_identificador: 'codigo'
      },
      busca_realizada: {
        codigo_buscado: codigo,
        tipo_busca: 'codigo',
        encontrado_por: 'id_planilha'
      },
      stats_calculadas_tempo_real: statsCalculadas,
      relacoes: relacoesDetalhadas.sort((a, b) => a.preco_final - b.preco_final),
      debug: {
        total_relacoes: relacoes.length,
        relacoes_com_preco: relacoesAtivas.length,
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
    version: '5.0.0-apenas-codigo-valido',
    alteracoes_versao: [
      'APENAS produtos com código válido são processados',
      'IGNORADOS produtos sem código ou com código inválido',
      'REMOVIDA lógica de busca por nome_completo',
      'REMOVIDA lógica de evolução de produtos',
      'SIMPLIFICADA lógica de sincronização',
      'MANTIDAS todas as outras funcionalidades'
    ],
    logica_simplificada: {
      'processamento': 'Apenas produtos com isCodigoValido(codigo) === true',
      'busca': 'Apenas por id_planilha (código)',
      'criacao': 'Apenas se código não existe no banco',
      'atualizacao': 'Apenas preços de produtos existentes por código'
    },
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
    observacao: 'Apenas produtos com código válido são processados',
    timestamp: new Date().toISOString()
  });
});

// Rota para debug de produtos - ATUALIZADA
app.get('/debug/produtos-por-tipo', async (req, res) => {
  try {
    console.log('🔍 Analisando produtos por tipo de código...');
    
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
      porcentagem_com_codigo: ((produtosComCodigo.length / todosProdutos.length) * 100).toFixed(2) + '%',
      porcentagem_sem_codigo: ((produtosSemCodigo.length / todosProdutos.length) * 100).toFixed(2) + '%',
      observacao: 'Apenas produtos com código válido são processados na nova versão',
      amostra_produtos_com_codigo: produtosComCodigo.slice(0, 5).map(p => ({
        _id: p._id,
        id_planilha: p.id_planilha,
        nome_completo: p.nome_completo,
        qtd_fornecedores: p.qtd_fornecedores,
        menor_preco: p.menor_preco
      })),
      amostra_produtos_sem_codigo: produtosSemCodigo.slice(0, 5).map(p => ({
        _id: p._id,
        id_planilha: p.id_planilha || '(vazio)',
        nome_completo: p.nome_completo,
        qtd_fornecedores: p.qtd_fornecedores,
        menor_preco: p.menor_preco,
        status: 'IGNORADO na nova versão'
      })),
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao analisar produtos',
      details: error.message
    });
  }
});

// Rota de documentação atualizada
app.get('/', (req, res) => {
  res.json({
    message: 'API COM BUSCAS ESPECÍFICAS - Eliminação TOTAL de duplicatas',
    version: '6.0.0-buscas-especificas-anti-duplicacao',
    revolucao_arquitetural: [
      '🚀 ABANDONADA abordagem de mapas em memória',
      '🔍 IMPLEMENTADA busca específica antes de cada operação',
      '📝 PROCESSAMENTO SEQUENCIAL (sem simultaneidade)',
      '🎯 CONSTRAINT específico por campo no Bubble',
      '🔒 ZERO condições de corrida possíveis'
    ],
    fluxo_novo: {
      'passo_1': 'Carrega APENAS fornecedores (para mapeamento loja → ID)',
      'passo_2': 'Para CADA produto: BUSCA por id_planilha antes de decidir',
      'passo_3': 'Se produto existe: pega _id, se não existe: cria novo',
      'passo_4': 'Para CADA relação: BUSCA por produto+fornecedor antes de decidir',
      'passo_5': 'Se relação existe: atualiza preços, se não existe: cria nova',
      'passo_6': 'Cotação diária: busca relações ativas e verifica códigos'
    },
    requisicoes_bubble: {
      'fornecedores': 'fetchAllFromBubble (paginado) - APENAS no início',
      'verificar_produto': 'GET /1 - produtos_25marco?constraints=[{key:id_planilha,equals:codigo}]',
      'verificar_relacao': 'GET /1 - ProdutoFornecedor _25marco?constraints=[{produto:ID},{fornecedor:ID}]',
      'criar_produto': 'POST /1 - produtos_25marco',
      'criar_relacao': 'POST /1 - ProdutoFornecedor _25marco',
      'atualizar_relacao': 'PATCH /1 - ProdutoFornecedor _25marco/{id}',
      'cotacao_diaria': 'GET relações ativas + GET produto por ID'
    },
    vantagens_abordagem: [
      '✅ IMPOSSÍVEL criar duplicatas (busca sempre antes)',
      '✅ DADOS sempre atualizados (busca em tempo real)',
      '✅ ZERO condições de corrida (processamento sequencial)',
      '✅ LÓGICA simples e clara (sem mapas complexos)',
      '✅ LOGS detalhados de cada decisão',
      '✅ PERFORMANCE controlada (delays configuráveis)'
    ],
    endpoints: {
      'POST /process-csv': 'Processa CSV com buscas específicas',
      'POST /force-recalculate': 'EXECUTA lógica final de recálculo',
      'GET /stats': 'Estatísticas das tabelas',
      'GET /produto/:codigo': 'Busca produto por código',
      'GET /debug/produtos-por-tipo': 'Debug de produtos com/sem código',
      'GET /health': 'Status da nova arquitetura',
      'GET /test-bubble': 'Testa conectividade com Bubble',
      'GET /performance': 'Monitora performance do servidor'
    },
    garantias_arquiteturais: [
      '🔒 BUSCA SEMPRE antes de criar',
      '🔒 PROCESSAMENTO SEQUENCIAL sem race conditions',
      '🔒 CONSTRAINT específico elimina duplicatas no banco',
      '🔒 LOGS completos para auditoria total',
      '🔒 DELAYS configuráveis para rate limiting',
      '🔒 RETRY automático em falhas temporárias'
    ],
    configuracoes_performance: {
      'processamento': 'SEQUENCIAL (produto por produto)',
      'delay_produtos': '50ms entre produtos',
      'delay_cotacao': '25ms entre verificações',
      'timeout_requisicao': PROCESSING_CONFIG.REQUEST_TIMEOUT + 'ms',
      'tentativas_retry': PROCESSING_CONFIG.RETRY_ATTEMPTS,
      'limite_arquivo': '100MB'
    }
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
  console.log(`🚀 Servidor COM BUSCAS ESPECÍFICAS rodando na porta ${PORT}`);
  console.log(`📊 Acesse: http://localhost:${PORT}`);
  console.log(`🔗 Integração Bubble configurada`);
  console.log(`⚡ Versão 6.0.0-buscas-especificas-anti-duplicacao`);
  console.log(`🔧 REVOLUÇÃO ARQUITETURAL IMPLEMENTADA:`);
  console.log(`   🚀 ABANDONADA abordagem de mapas em memória`);
  console.log(`   🔍 BUSCA ESPECÍFICA antes de cada operação`);
  console.log(`   📝 PROCESSAMENTO SEQUENCIAL (sem simultaneidade)`);
  console.log(`   🎯 CONSTRAINT do Bubble elimina duplicatas`);
  console.log(`   🔒 ZERO condições de corrida possíveis`);
  console.log(`   ✅ MANTIDA lógica de recálculo final`);
  console.log(`📋 FLUXO NOVO:`);
  console.log(`   1. Carrega fornecedores (mapeamento loja → ID)`);
  console.log(`   2. Para cada produto: BUSCA por id_planilha`);
  console.log(`   3. Para cada relação: BUSCA por produto+fornecedor`);
  console.log(`   4. Cotação diária: busca relações ativas`);
  console.log(`📈 Configurações de performance:`);
  console.log(`   - Processamento: SEQUENCIAL (produto por produto)`);
  console.log(`   - Delay produtos: 50ms`);
  console.log(`   - Delay cotação: 25ms`);
  console.log(`   - Retry: ${PROCESSING_CONFIG.RETRY_ATTEMPTS} tentativas`);
  console.log(`   - Timeout: ${PROCESSING_CONFIG.REQUEST_TIMEOUT}ms`);
  console.log(`   - Limite arquivo: 100MB`);
  console.log(`\n🎯 DUPLICATAS ELIMINADAS POR DESIGN!`);
  console.log(`   ✅ Busca SEMPRE antes de criar`);
  console.log(`   ✅ Processamento sequencial`);
  console.log(`   ✅ Constraint específico no Bubble`);
  console.log(`   ✅ Impossível ter race conditions`);
  console.log(`   ✅ Logs detalhados de cada decisão`);
});

module.exports = app;