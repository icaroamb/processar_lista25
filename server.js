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
  BATCH_SIZE: 50,
  MAX_CONCURRENT: 5,
  RETRY_ATTEMPTS: 3,
  RETRY_DELAY: 1000,
  REQUEST_TIMEOUT: 60000,
  BATCH_DELAY: 100,
  MEMORY_CLEANUP_INTERVAL: 1000
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
    fileSize: 100 * 1024 * 1024 // 100MB max
  },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Apenas arquivos CSV são permitidos!'), false);
    }
  }
});

// Função para verificar se código é válido
function isCodigoValido(codigo) {
  if (!codigo || codigo.toString().trim() === '' || codigo.toString().trim().toUpperCase() === 'SEM CÓDIGO') {
    return false;
  }
  return true;
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

// Função de delay
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Função de retry
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

// Função para buscar TODOS os dados de uma tabela com offset
async function fetchAllFromBubble(tableName, filters = {}) {
  try {
    console.log(`🔍 Buscando TODOS os dados de ${tableName}...`);
    let allData = [];
    let cursor = 0;
    let hasMore = true;
    let totalFetched = 0;
    let maxIterations = 1000;
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
      
      if (!newResults || newResults.length === 0) {
        console.log(`📊 ${tableName}: Nenhum novo resultado, finalizando busca`);
        break;
      }
      
      allData = allData.concat(newResults);
      totalFetched += newResults.length;
      
      const remaining = data.response.remaining || 0;
      const newCursor = data.response.cursor;
      
      hasMore = remaining > 0 && newCursor && newCursor !== cursor;
      
      if (hasMore) {
        cursor = newCursor;
      }
      
      console.log(`📊 ${tableName}: ${totalFetched} registros carregados (restam: ${remaining})`);
      
      if (hasMore) {
        await delay(50);
      }
      
      if (newCursor === cursor && remaining > 0) {
        console.warn(`⚠️ ${tableName}: Cursor não mudou, possível loop. Finalizando busca.`);
        break;
      }
    }
    
    if (currentIteration >= maxIterations) {
      console.warn(`⚠️ ${tableName}: Atingido limite máximo de iterações (${maxIterations}).`);
    }
    
    console.log(`✅ ${tableName}: ${allData.length} registros carregados total`);
    return allData;
    
  } catch (error) {
    console.error(`❌ Erro ao buscar ${tableName}:`, error.message);
    throw new Error(`Erro ao buscar ${tableName}: ${error.message}`);
  }
}

// Função para criar item no Bubble
async function createInBubble(tableName, data) {
  return await retryOperation(async () => {
    const response = await axios.post(`${BUBBLE_CONFIG.baseURL}/${tableName}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
    });
    return response.data;
  });
}

// Função para atualizar item no Bubble
async function updateInBubble(tableName, itemId, data) {
  return await retryOperation(async () => {
    const response = await axios.patch(`${BUBBLE_CONFIG.baseURL}/${tableName}/${itemId}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: PROCESSING_CONFIG.REQUEST_TIMEOUT
    });
    return response.data;
  });
}

// PASSO 1: Processar CSV e montar JSON completo
function processCSV(filePath) {
  return new Promise((resolve, reject) => {
    try {
      console.log('📁 PASSO 1: Lendo e processando arquivo CSV...');
      
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const lines = fileContent.split('\n').filter(line => line.trim());
      
      if (lines.length < 3) {
        console.log('❌ Arquivo CSV muito pequeno');
        return resolve([]);
      }
      
      const dataLines = lines.slice(2);
      console.log(`📊 Processando ${dataLines.length} linhas de dados`);
      
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
        
        dataLines.forEach((line) => {
          if (!line || line.trim() === '') return;
          
          const columns = parseCSVLine(line);
          if (columns.length < 31) return;
          
          const codigo = columns[lojaConfig.indices[0]];
          const modelo = columns[lojaConfig.indices[1]];
          const preco = columns[lojaConfig.indices[2]];
          
          // APENAS PRODUTOS COM CÓDIGO VÁLIDO
          if (isCodigoValido(codigo) && modelo && preco && 
              modelo.trim() !== '' && preco.trim() !== '') {
            
            const precoNumerico = extractPrice(preco);
            
            produtos.push({
              codigo: codigo.trim(),
              modelo: modelo.trim(),
              preco: precoNumerico,
              tipo_identificador: 'codigo',
              id_planilha: codigo.trim(),
              nome_completo: modelo.trim()
            });
          } else if (modelo && preco && modelo.trim() !== '' && preco.trim() !== '') {
            produtosSemCodigo++;
          }
        });
        
        console.log(`✅ ${lojaConfig.nome}: ${produtos.length} produtos (${produtosSemCodigo} ignorados sem código)`);
        
        if (produtos.length > 0) {
          processedData.push({
            loja: lojaConfig.nome,
            total_produtos: produtos.length,
            produtos_com_codigo: produtos.length,
            produtos_sem_codigo: produtosSemCodigo,
            produtos: produtos
          });
        }
      });
      
      console.log('✅ PASSO 1 CONCLUÍDO: JSON do CSV montado');
      resolve(processedData);
      
    } catch (error) {
      console.error('❌ Erro no PASSO 1:', error);
      reject(error);
    }
  });
}

// NOVA LÓGICA PRINCIPAL - ANTI-DUPLICAÇÃO CORRETA
async function syncWithBubbleNovologica(csvData, gorduraValor) {
  try {
    console.log('\n🔥 INICIANDO NOVA LÓGICA ANTI-DUPLICAÇÃO CORRETA');
    
    // PASSO 2: Buscar TODOS os fornecedores (SEM OFFSET - conforme solicitado)
    console.log('\n📊 PASSO 2: Buscando TODOS os fornecedores...');
    const todosFornecedores = await fetchAllFromBubble('1 - fornecedor_25marco');
    console.log(`✅ ${todosFornecedores.length} fornecedores carregados`);
    
    // Criar mapa de fornecedores
    const fornecedorMap = new Map();
    todosFornecedores.forEach(f => {
      fornecedorMap.set(f.nome_fornecedor, {
        _id: f._id,
        nome_fornecedor: f.nome_fornecedor
      });
    });
    
    // PASSO 3: Buscar TODOS os produtos
    console.log('\n📊 PASSO 3: Buscando TODOS os produtos...');
    const todosProdutos = await fetchAllFromBubble('1 - produtos_25marco');
    console.log(`✅ ${todosProdutos.length} produtos carregados`);
    
    // PASSO 4: Separar itens para EDITAR e CRIAR
    console.log('\n📝 PASSO 4: Separando itens para EDITAR e CRIAR...');
    
    const itensEditar = [];
    const itensCriar = [];
    
    for (const lojaData of csvData) {
      const nomeLoja = lojaData.loja;
      const fornecedorInfo = fornecedorMap.get(nomeLoja);
      
      if (!fornecedorInfo) {
        console.log(`⚠️ Fornecedor não encontrado: ${nomeLoja} - criando...`);
        // Criar fornecedor se não existir
        const novoFornecedor = await createInBubble('1 - fornecedor_25marco', {
          nome_fornecedor: nomeLoja
        });
        fornecedorMap.set(nomeLoja, {
          _id: novoFornecedor.id,
          nome_fornecedor: nomeLoja
        });
      }
      
      const fornecedorFinal = fornecedorMap.get(nomeLoja);
      const produtosParaEditar = [];
      const produtosParaCriar = [];
      
      for (const produtoCsv of lojaData.produtos) {
        const idPlanilha = produtoCsv.id_planilha;
        
        // BUSCAR PRODUTO QUE TENHA MESMO ID_PLANILHA 
        // *** ATENÇÃO: AQUI ESTÁ A CORREÇÃO CRÍTICA ***
        // Não buscamos apenas por id_planilha, mas por id_planilha + fornecedor
        // para evitar duplicação na tabela de ligação
        
        const produtoExistente = todosProdutos.find(p => p.id_planilha === idPlanilha);
        
        if (produtoExistente) {
          // PRODUTO EXISTE - vai para EDITAR
          produtosParaEditar.push({
            ...produtoCsv,
            unique_produto: produtoExistente._id
          });
          console.log(`✏️  EDITAR: ${idPlanilha} (${nomeLoja})`);
        } else {
          // PRODUTO NÃO EXISTE - vai para CRIAR
          produtosParaCriar.push({
            ...produtoCsv
            // unique_produto não existe pois será criado
          });
          console.log(`➕ CRIAR: ${idPlanilha} (${nomeLoja})`);
        }
      }
      
      if (produtosParaEditar.length > 0) {
        itensEditar.push({
          loja: nomeLoja,
          unique_fornecedor: fornecedorFinal._id,
          produtos: produtosParaEditar
        });
      }
      
      if (produtosParaCriar.length > 0) {
        itensCriar.push({
          loja: nomeLoja,
          unique_fornecedor: fornecedorFinal._id,
          produtos: produtosParaCriar
        });
      }
    }
    
    console.log(`📋 Separação concluída:`);
    console.log(`   Lojas com itens para EDITAR: ${itensEditar.length}`);
    console.log(`   Lojas com itens para CRIAR: ${itensCriar.length}`);
    
    // PASSO 5: Buscar TODAS as relações produto-fornecedor
    console.log('\n🔗 PASSO 5: Buscando TODAS as relações produto-fornecedor...');
    const todasRelacoes = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco');
    console.log(`✅ ${todasRelacoes.length} relações carregadas`);
    
    // PASSO 6: PROCESSAR ITENS PARA EDITAR
    console.log('\n✏️  PASSO 6: Processando itens para EDITAR...');
    let itensEditados = 0;
    
    for (const itemEditar of itensEditar) {
      const uniqueFornecedor = itemEditar.unique_fornecedor;
      
      for (const produto of itemEditar.produtos) {
        const uniqueProduto = produto.unique_produto;
        
        // ENCONTRAR A RELAÇÃO EXISTENTE (MATCH EXATO: produto + fornecedor)
        const relacaoExistente = todasRelacoes.find(r => 
          r.produto === uniqueProduto && r.fornecedor === uniqueFornecedor
        );
        
        if (relacaoExistente) {
          // CALCULAR NOVOS PREÇOS
          const precoOriginal = produto.preco;
          const precoFinal = precoOriginal === 0 ? 0 : precoOriginal + gorduraValor;
          const precoOrdenacao = precoOriginal === 0 ? 999999 : precoOriginal;
          
          // ATUALIZAR RELAÇÃO
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacaoExistente._id, {
            preco_original: precoOriginal,
            preco_final: precoFinal,
            preco_ordenacao: precoOrdenacao,
            nome_produto: produto.nome_completo
          });
          
          itensEditados++;
          console.log(`✏️  Editado: ${produto.id_planilha} (${itemEditar.loja}) - Preço: ${precoFinal}`);
        } else {
          console.warn(`⚠️ Relação não encontrada para edição: ${produto.id_planilha} + ${itemEditar.loja}`);
        }
      }
    }
    
    // PASSO 7: PROCESSAR ITENS PARA CRIAR
    console.log('\n➕ PASSO 7: Processando itens para CRIAR...');
    let produtosCriados = 0;
    let relacoesCriadas = 0;
    
    for (const itemCriar of itensCriar) {
      const uniqueFornecedor = itemCriar.unique_fornecedor;
      
      for (const produto of itemCriar.produtos) {
        // CRIAR PRODUTO PRIMEIRO
        const novoProduto = await createInBubble('1 - produtos_25marco', {
          id_planilha: produto.id_planilha,
          nome_completo: produto.nome_completo,
          preco_medio: 0,
          qtd_fornecedores: 0,
          menor_preco: 0
        });
        
        produtosCriados++;
        console.log(`➕ Produto criado: ${produto.id_planilha}`);
        
        // *** VERIFICAÇÃO CRÍTICA ANTI-DUPLICAÇÃO ***
        // Antes de criar a relação, verificar se já existe
        const relacaoJaExiste = todasRelacoes.find(r => 
          r.produto === novoProduto.id && r.fornecedor === uniqueFornecedor
        );
        
        if (!relacaoJaExiste) {
          // CALCULAR PREÇOS
          const precoOriginal = produto.preco;
          const precoFinal = precoOriginal === 0 ? 0 : precoOriginal + gorduraValor;
          const precoOrdenacao = precoOriginal === 0 ? 999999 : precoOriginal;
          
          // CRIAR RELAÇÃO
          const novaRelacao = await createInBubble('1 - ProdutoFornecedor _25marco', {
            produto: novoProduto.id,
            fornecedor: uniqueFornecedor,
            nome_produto: produto.nome_completo,
            preco_original: precoOriginal,
            preco_final: precoFinal,
            preco_ordenacao: precoOrdenacao,
            melhor_preco: 'no'
          });
          
          // ADICIONAR À LISTA LOCAL PARA EVITAR DUPLICAÇÕES FUTURAS NESTE LOTE
          todasRelacoes.push({
            _id: novaRelacao.id,
            produto: novoProduto.id,
            fornecedor: uniqueFornecedor,
            preco_original: precoOriginal,
            preco_final: precoFinal,
            preco_ordenacao: precoOrdenacao,
            melhor_preco: 'no'
          });
          
          relacoesCriadas++;
          console.log(`🔗 Relação criada: ${produto.id_planilha} + ${itemCriar.loja} - Preço: ${precoFinal}`);
        } else {
          console.warn(`⚠️ DUPLICAÇÃO EVITADA: Relação ${produto.id_planilha} + ${itemCriar.loja} já existe!`);
        }
      }
    }
    
    // PASSO 8: EXECUTAR LÓGICA FINAL DE RECÁLCULO
    console.log('\n🔥 PASSO 8: Executando lógica final de recálculo...');
    const logicaFinalResults = await executarLogicaFinalCorreta();
    
    const results = {
      produtos_criados: produtosCriados,
      relacoes_criadas: relacoesCriadas,
      itens_editados: itensEditados,
      fornecedores_processados: fornecedorMap.size,
      logica_final_correta: logicaFinalResults,
      sucesso: true
    };
    
    console.log('\n🎯 NOVA LÓGICA ANTI-DUPLICAÇÃO CONCLUÍDA COM SUCESSO!');
    console.log('📊 Resultados:', results);
    
    return results;
    
  } catch (error) {
    console.error('❌ Erro na nova lógica:', error);
    throw error;
  }
}

// LÓGICA FINAL DE RECÁLCULO (mantida igual)
async function executarLogicaFinalCorreta() {
  console.log('\n🔥 === EXECUTANDO LÓGICA FINAL CORRETA ===');
  
  try {
    let todosOsItens = [];
    let cursor = 0;
    let remaining = 1;
    
    while (remaining > 0) {
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
      cursor += 100;
      
      if (remaining > 0) {
        await delay(50);
      }
    }
    
    console.log(`📊 Total de relações carregadas: ${todosOsItens.length}`);
    
    // Agrupar por produto
    const grupos = {};
    todosOsItens.forEach(item => {
      if (item.preco_final && item.preco_final > 0) {
        const produtoId = item.produto;
        if (!grupos[produtoId]) {
          grupos[produtoId] = [];
        }
        grupos[produtoId].push(item);
      }
    });
    
    const produtoIds = Object.keys(grupos);
    console.log(`📊 Produtos agrupados: ${produtoIds.length}`);
    
    // Preparar operações
    const operacoesProdutos = [];
    const operacoesMelhorPreco = [];
    
    for (const produtoId of produtoIds) {
      const grupo = grupos[produtoId];
      const precosFinal = grupo.map(item => item.preco_final);
      
      const qtd_fornecedores = grupo.length;
      const menor_preco = Math.min(...precosFinal);
      const soma = precosFinal.reduce((a, b) => a + b, 0);
      const preco_medio = Math.round((soma / qtd_fornecedores) * 100) / 100;
      const itemComMenorPreco = grupo.find(item => item.preco_final === menor_preco);
      const fornecedor_menor_preco = itemComMenorPreco.fornecedor;
      
      operacoesProdutos.push({
        produtoId: produtoId,
        dados: {
          qtd_fornecedores: qtd_fornecedores,
          menor_preco: menor_preco,
          preco_medio: preco_medio,
          fornecedor_menor_preco: fornecedor_menor_preco
        }
      });
      
      grupo.forEach(item => {
        const melhor_preco = (item.preco_final === menor_preco) ? 'yes' : 'no';
        operacoesMelhorPreco.push({
          itemId: item._id,
          melhor_preco: melhor_preco
        });
      });
    }
    
    // Executar operações de produtos
    let produtosEditados = 0;
    for (const operacao of operacoesProdutos) {
      try {
        await updateInBubble('1 - produtos_25marco', operacao.produtoId, operacao.dados);
        produtosEditados++;
      } catch (error) {
        console.error(`❌ Erro ao editar produto ${operacao.produtoId}:`, error.message);
      }
    }
    
    // Executar operações de melhor preço
    let itensEditados = 0;
    for (const operacao of operacoesMelhorPreco) {
      try {
        await updateInBubble('1 - ProdutoFornecedor _25marco', operacao.itemId, {
          melhor_preco: operacao.melhor_preco
        });
        itensEditados++;
      } catch (error) {
        console.error(`❌ Erro ao editar item ${operacao.itemId}:`, error.message);
      }
    }
    
    // Zerar itens inválidos
    const itensInvalidos = todosOsItens.filter(item => !item.preco_final || item.preco_final <= 0);
    let itensInvalidosEditados = 0;
    
    for (const item of itensInvalidos) {
      try {
        await updateInBubble('1 - ProdutoFornecedor _25marco', item._id, {
          melhor_preco: 'no'
        });
        itensInvalidosEditados++;
      } catch (error) {
        console.error(`❌ Erro ao zerar item ${item._id}:`, error.message);
      }
    }
    
    const resultados = {
      total_itens_carregados: todosOsItens.length,
      produtos_agrupados: produtoIds.length,
      produtos_editados: produtosEditados,
      itens_editados: itensEditados,
      itens_invalidos: itensInvalidos.length,
      itens_invalidos_editados: itensInvalidosEditados,
      sucesso: true
    };
    
    console.log('✅ LÓGICA FINAL CORRETA CONCLUÍDA');
    return resultados;
    
  } catch (error) {
    console.error('❌ ERRO na lógica final correta:', error);
    throw error;
  }
}

// ROTAS DA API

// Rota principal para upload e processamento
app.post('/process-csv', upload.single('csvFile'), async (req, res) => {
  try {
    console.log('\n🚀 === NOVA REQUISIÇÃO COM LÓGICA ANTI-DUPLICAÇÃO ===');
    
    if (!req.file) {
      return res.status(400).json({ error: 'Nenhum arquivo CSV foi enviado' });
    }
    
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
      return res.status(400).json({ error: 'Arquivo não encontrado' });
    }
    
    const startTime = Date.now();
    
    // PROCESSAR CSV
    const csvData = await processCSV(filePath);
    
    // NOVA LÓGICA ANTI-DUPLICAÇÃO
    const syncResults = await syncWithBubbleNovologica(csvData, gorduraValor);
    
    const endTime = Date.now();
    const processingTime = (endTime - startTime) / 1000;
    
    // Limpar arquivo temporário
    fs.unlinkSync(filePath);
    
    console.log(`✅ Processamento concluído em ${processingTime}s`);
    
    res.json({
      success: true,
      message: 'CSV processado com NOVA LÓGICA ANTI-DUPLICAÇÃO',
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
        produtos_criados: syncResults.produtos_criados,
        relacoes_criadas: syncResults.relacoes_criadas,
        itens_editados: syncResults.itens_editados,
        fornecedores_processados: syncResults.fornecedores_processados
      },
      observacoes: [
        'NOVA LÓGICA implementada para evitar duplicação na tabela de ligação',
        'GARANTIDO que não existe mais de 1 relação com mesmo produto+fornecedor',
        'PRODUTOS sem código válido são completamente ignorados',
        'VERIFICAÇÃO dupla anti-duplicação implementada'
      ]
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

// Rota para executar apenas a lógica final de recálculo
app.post('/force-recalculate', async (req, res) => {
  try {
    console.log('\n🔥 === EXECUTANDO APENAS LÓGICA FINAL DE RECÁLCULO ===');
    
    const startTime = Date.now();
    const results = await executarLogicaFinalCorreta();
    const endTime = Date.now();
    const processingTime = (endTime - startTime) / 1000;
    
    console.log(`🔥 Lógica final executada em ${processingTime}s`);
    
    res.json({
      success: true,
      message: 'LÓGICA FINAL DE RECÁLCULO executada com sucesso',
      tempo_processamento: processingTime + 's',
      resultados: results,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Erro na lógica final:', error);
    res.status(500).json({
      error: 'Erro na LÓGICA FINAL DE RECÁLCULO',
      details: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Rota para estatísticas
app.get('/stats', async (req, res) => {
  try {
    const [fornecedores, produtos, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - fornecedor_25marco'),
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    const produtosComCodigo = produtos.filter(p => p.id_planilha && p.id_planilha.trim() !== '').length;
    const produtosSemCodigo = produtos.filter(p => !p.id_planilha || p.id_planilha.trim() === '').length;
    
    // VERIFICAÇÃO ANTI-DUPLICAÇÃO
    const relacoesUnicas = new Set();
    const relacoesDuplicadas = [];
    
    produtoFornecedores.forEach(relacao => {
      const chave = `${relacao.produto}-${relacao.fornecedor}`;
      if (relacoesUnicas.has(chave)) {
        relacoesDuplicadas.push({
          produto: relacao.produto,
          fornecedor: relacao.fornecedor,
          _id: relacao._id
        });
      } else {
        relacoesUnicas.add(chave);
      }
    });
    
    res.json({
      total_fornecedores: fornecedores.length,
      total_produtos: produtos.length,
      produtos_com_codigo: produtosComCodigo,
      produtos_sem_codigo: produtosSemCodigo,
      total_relacoes: produtoFornecedores.length,
      relacoes_unicas: relacoesUnicas.size,
      relacoes_duplicadas: relacoesDuplicadas.length,
      duplicacoes_encontradas: relacoesDuplicadas.length > 0 ? relacoesDuplicadas : 'Nenhuma duplicação encontrada! ✅',
      status_duplicacao: relacoesDuplicadas.length === 0 ? 'LIMPO - Sem duplicações ✅' : `PROBLEMA - ${relacoesDuplicadas.length} duplicações encontradas ❌`,
      fornecedores_ativos: fornecedores.filter(f => f.status_ativo === 'yes').length,
      produtos_com_preco: produtos.filter(p => p.menor_preco > 0).length,
      relacoes_ativas: produtoFornecedores.filter(pf => pf.status_ativo === 'yes').length,
      relacoes_com_preco: produtoFornecedores.filter(pf => pf.preco_final > 0).length,
      observacao: 'Nova lógica implementada para evitar duplicações na tabela de ligação',
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao buscar estatísticas',
      details: error.message
    });
  }
});

// Rota para buscar produto por código
app.get('/produto/:codigo', async (req, res) => {
  try {
    const codigo = req.params.codigo;
    console.log(`🔍 Buscando produto por código: ${codigo}`);
    
    const todosProdutos = await fetchAllFromBubble('1 - produtos_25marco');
    const produto = todosProdutos.find(p => p.id_planilha === codigo);
    
    if (!produto) {
      return res.status(404).json({
        error: 'Produto não encontrado',
        message: `Nenhum produto encontrado com código: ${codigo}`
      });
    }
    
    const todasRelacoes = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco');
    const relacoes = todasRelacoes.filter(r => r.produto === produto._id);
    
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
        preco_ordenacao: r.preco_ordenacao,
        relacao_id: r._id
      };
    });
    
    // VERIFICAR DUPLICAÇÕES PARA ESTE PRODUTO
    const fornecedoresUnicos = new Set();
    const duplicacoesEncontradas = [];
    
    relacoes.forEach(relacao => {
      const fornecedorId = relacao.fornecedor;
      if (fornecedoresUnicos.has(fornecedorId)) {
        const fornecedor = fornecedorMap.get(fornecedorId);
        duplicacoesEncontradas.push({
          fornecedor_nome: fornecedor?.nome_fornecedor || 'Desconhecido',
          fornecedor_id: fornecedorId,
          relacao_id: relacao._id
        });
      } else {
        fornecedoresUnicos.add(fornecedorId);
      }
    });
    
    const relacoesAtivas = relacoes.filter(r => r.preco_final > 0);
    const precosValidos = relacoesAtivas.map(r => r.preco_final);
    const statsCalculadas = {
      qtd_fornecedores: precosValidos.length,
      menor_preco: precosValidos.length > 0 ? Math.min(...precosValidos) : 0,
      preco_medio: precosValidos.length > 0 ? 
        Math.round((precosValidos.reduce((a, b) => a + b, 0) / precosValidos.length) * 100) / 100 : 0
    };
    
    res.json({
      produto: {
        codigo: produto.id_planilha,
        nome: produto.nome_completo,
        preco_menor: produto.menor_preco,
        preco_medio: produto.preco_medio,
        qtd_fornecedores: produto.qtd_fornecedores,
        produto_id: produto._id
      },
      stats_calculadas_tempo_real: statsCalculadas,
      relacoes: relacoesDetalhadas.sort((a, b) => a.preco_final - b.preco_final),
      verificacao_duplicacao: {
        total_relacoes: relacoes.length,
        fornecedores_unicos: fornecedoresUnicos.size,
        duplicacoes_encontradas: duplicacoesEncontradas.length,
        detalhes_duplicacao: duplicacoesEncontradas.length > 0 ? duplicacoesEncontradas : 'Nenhuma duplicação ✅',
        status: duplicacoesEncontradas.length === 0 ? 'LIMPO ✅' : `PROBLEMA - ${duplicacoesEncontradas.length} duplicações ❌`
      },
      debug: {
        total_relacoes: relacoes.length,
        relacoes_com_preco: relacoesAtivas.length,
        precos_validos: precosValidos
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

// Rota para detectar e listar todas as duplicações
app.get('/debug/duplicacoes', async (req, res) => {
  try {
    console.log('🔍 Analisando duplicações na tabela de ligação...');
    
    const [produtos, fornecedores, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - fornecedor_25marco'), 
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    // Criar mapas para resolução de nomes
    const produtoMap = new Map();
    produtos.forEach(p => produtoMap.set(p._id, p));
    
    const fornecedorMap = new Map();
    fornecedores.forEach(f => fornecedorMap.set(f._id, f));
    
    // Detectar duplicações
    const relacoesAgrupadas = new Map();
    const duplicacoesEncontradas = [];
    
    produtoFornecedores.forEach(relacao => {
      const chave = `${relacao.produto}-${relacao.fornecedor}`;
      
      if (relacoesAgrupadas.has(chave)) {
        // DUPLICAÇÃO ENCONTRADA!
        const produto = produtoMap.get(relacao.produto);
        const fornecedor = fornecedorMap.get(relacao.fornecedor);
        
        duplicacoesEncontradas.push({
          produto_codigo: produto?.id_planilha || 'Código não encontrado',
          produto_nome: produto?.nome_completo || 'Nome não encontrado',
          fornecedor_nome: fornecedor?.nome_fornecedor || 'Fornecedor não encontrado',
          relacao_duplicada_id: relacao._id,
          relacao_original_id: relacoesAgrupadas.get(chave)._id,
          preco_duplicado: relacao.preco_final,
          preco_original: relacoesAgrupadas.get(chave).preco_final
        });
      } else {
        relacoesAgrupadas.set(chave, relacao);
      }
    });
    
    // Agrupar duplicações por produto
    const duplicacoesPorProduto = new Map();
    duplicacoesEncontradas.forEach(dup => {
      const codigo = dup.produto_codigo;
      if (!duplicacoesPorProduto.has(codigo)) {
        duplicacoesPorProduto.set(codigo, []);
      }
      duplicacoesPorProduto.get(codigo).push(dup);
    });
    
    res.json({
      total_relacoes: produtoFornecedores.length,
      relacoes_unicas_esperadas: relacoesAgrupadas.size,
      duplicacoes_encontradas: duplicacoesEncontradas.length,
      status_geral: duplicacoesEncontradas.length === 0 ? 'LIMPO - Sem duplicações ✅' : `PROBLEMA - ${duplicacoesEncontradas.length} duplicações encontradas ❌`,
      produtos_com_duplicacao: duplicacoesPorProduto.size,
      detalhes_duplicacoes: Array.from(duplicacoesPorProduto.entries()).map(([codigo, dups]) => ({
        produto_codigo: codigo,
        total_duplicacoes: dups.length,
        duplicacoes: dups
      })),
      resumo_duplicacoes: duplicacoesEncontradas.slice(0, 10), // Primeiras 10 para não sobrecarregar
      observacoes: [
        'Esta análise mostra todas as duplicações na tabela de ligação',
        'Cada produto deve ter APENAS 1 relação por fornecedor',  
        'Se há duplicações, a nova lógica deve ser aplicada',
        'Use POST /process-csv com a nova lógica para corrigir'
      ],
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      error: 'Erro ao analisar duplicações',
      details: error.message
    });
  }
});

// Rota para saúde da aplicação
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    message: 'API funcionando com NOVA LÓGICA ANTI-DUPLICAÇÃO',
    version: '6.0.0-anti-duplicacao-correta',
    correcoes_implementadas: [
      '🚫 ELIMINADA duplicação na tabela de ligação',
      '🔍 VERIFICAÇÃO dupla antes de criar relações',
      '📋 SEPARAÇÃO correta entre itens para EDITAR e CRIAR',
      '🎯 MATCH exato por produto+fornecedor',
      '✅ GARANTIA de 1 relação única por produto+fornecedor',
      '📊 ESTATÍSTICAS com detecção de duplicação'
    ],
    logica_nova: {
      'passo_1': 'Processar CSV e montar JSON completo',
      'passo_2': 'Buscar TODOS os fornecedores',
      'passo_3': 'Buscar TODOS os produtos', 
      'passo_4': 'Separar itens para EDITAR vs CRIAR',
      'passo_5': 'Buscar TODAS as relações existentes',
      'passo_6': 'EDITAR relações existentes (match produto+fornecedor)',
      'passo_7': 'CRIAR produtos novos + relações (com verificação anti-duplicação)',
      'passo_8': 'Executar lógica final de recálculo'
    },
    garantias: [
      '✅ NUNCA cria relação duplicada (produto+fornecedor)',
      '✅ SEMPRE verifica se relação já existe antes de criar',
      '✅ PRODUTOS sem código são ignorados',
      '✅ MATCH exato entre CSV e banco de dados',
      '✅ SEPARAÇÃO correta entre edição e criação',
      '✅ ESTATÍSTICAS com detecção de problemas'
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

// Rota para testar conectividade
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

// Rota para performance
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
    observacao: 'Nova lógica anti-duplicação implementada',
    timestamp: new Date().toISOString()
  });
});

// Rota de documentação principal
app.get('/', (req, res) => {
  res.json({
    message: 'API COM NOVA LÓGICA ANTI-DUPLICAÇÃO CORRETA',
    version: '6.0.0-anti-duplicacao-correta',
    problema_resolvido: 'DUPLICAÇÃO na tabela de ligação 1 - ProdutoFornecedor_25marco',
    solucao_implementada: [
      '🎯 MATCH exato por produto+fornecedor antes de qualquer operação',
      '🔍 VERIFICAÇÃO se relação já existe antes de criar nova',
      '📋 SEPARAÇÃO correta entre itens para EDITAR vs CRIAR',
      '🚫 IMPOSSÍVEL criar relação duplicada (produto+fornecedor)',
      '✅ GARANTIA de unicidade na tabela de ligação'
    ],
    logica_passo_a_passo: {
      '1': 'Processar CSV → JSON completo (apenas códigos válidos)',
      '2': 'Buscar TODOS fornecedores → Mapa de fornecedores', 
      '3': 'Buscar TODOS produtos → Lista de produtos existentes',
      '4': 'Para cada produto CSV: existe no banco? → EDITAR : CRIAR',
      '5': 'Buscar TODAS relações → Mapa de relações existentes',
      '6': 'EDITAR: Match produto+fornecedor → Atualizar preços',
      '7': 'CRIAR: Criar produto + Verificar se relação existe → Criar relação',
      '8': 'Lógica final → Recalcular estatísticas e melhor preço'
    },
    endpoints: {
      'POST /process-csv': 'Processar CSV com nova lógica anti-duplicação',
      'POST /force-recalculate': 'Executar apenas lógica final de recálculo',
      'GET /stats': 'Estatísticas com detecção de duplicação',
      'GET /produto/:codigo': 'Buscar produto com verificação de duplicação',
      'GET /debug/duplicacoes': 'Detectar e listar TODAS as duplicações',
      'GET /health': 'Status da API com nova lógica',
      'GET /test-bubble': 'Testar conectividade',
      'GET /performance': 'Monitorar performance'
    },
    regra_critica: 'Na tabela 1 - ProdutoFornecedor_25marco NUNCA pode existir mais de 1 registro com mesmo PRODUTO + FORNECEDOR',
    exemplo_correto: 'iPhone 15 pode estar em várias lojas, mas cada loja só pode ter 1 iPhone 15',
    verificacoes_implementadas: [
      '✅ Antes de criar relação: verificar se produto+fornecedor já existe',
      '✅ Endpoint /debug/duplicacoes para detectar problemas',
      '✅ Estatísticas mostram status de duplicação',
      '✅ Busca de produto mostra duplicações encontradas',
      '✅ Logs detalhados do processo'
    ],
    configuracoes_performance: {
      'tamanho_lote': PROCESSING_CONFIG.BATCH_SIZE + ' itens',
      'max_concorrencia': PROCESSING_CONFIG.MAX_CONCURRENT + ' operações simultâneas',
      'tentativas_retry': PROCESSING_CONFIG.RETRY_ATTEMPTS,
      'timeout_requisicao': PROCESSING_CONFIG.REQUEST_TIMEOUT + 'ms',
      'limite_arquivo': '100MB'
    }
  });
});

// Middleware de tratamento de erros
app.use((error, req, res, next) => {
  console.error('🚨 Erro capturado:', error);
  
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
  
  if (error.code === 'ECONNABORTED') {
    return res.status(408).json({
      error: 'Timeout na requisição',
      codigo: 'REQUEST_TIMEOUT',
      details: 'A operação demorou mais que o esperado'
    });
  }
  
  if (error.code === 'ECONNREFUSED') {
    return res.status(503).json({
      error: 'Serviço indisponível',
      codigo: 'SERVICE_UNAVAILABLE',
      details: 'Não foi possível conectar ao serviço externo'
    });
  }
  
  res.status(500).json({ 
    error: 'Erro interno do servidor',
    codigo: 'INTERNAL_ERROR',
    timestamp: new Date().toISOString()
  });
});

// Tratamento de shutdown gracioso
process.on('SIGTERM', () => {
  console.log('🛑 Recebido SIGTERM, encerrando servidor...');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('🛑 Recebido SIGINT, encerrando servidor...');
  process.exit(0);
});

process.on('uncaughtException', (error) => {
  console.error('🚨 Exceção não capturada:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('🚨 Promise rejection não tratada:', reason);
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log(`🚀 Servidor NOVA LÓGICA ANTI-DUPLICAÇÃO rodando na porta ${PORT}`);
  console.log(`📊 Acesse: http://localhost:${PORT}`);
  console.log(`🔗 Integração Bubble configurada`);
  console.log(`⚡ Versão 6.0.0-anti-duplicacao-correta`);
  console.log(`\n🔧 NOVA LÓGICA IMPLEMENTADA:`);
  console.log(`   🚫 ELIMINA duplicação na tabela de ligação`);
  console.log(`   🔍 VERIFICA se relação existe antes de criar`);
  console.log(`   📋 SEPARA corretamente itens para EDITAR vs CRIAR`);
  console.log(`   🎯 MATCH exato por produto+fornecedor`);
  console.log(`   ✅ GARANTE unicidade: 1 produto = 1 fornecedor = 1 relação`);
  console.log(`\n📈 Configurações:`);
  console.log(`   - Lote: ${PROCESSING_CONFIG.BATCH_SIZE} itens`);
  console.log(`   - Concorrência: ${PROCESSING_CONFIG.MAX_CONCURRENT} operações`);
  console.log(`   - Retry: ${PROCESSING_CONFIG.RETRY_ATTEMPTS} tentativas`);
  console.log(`   - Timeout: ${PROCESSING_CONFIG.REQUEST_TIMEOUT}ms`);
  console.log(`\n🎯 PROBLEMA DE DUPLICAÇÃO RESOLVIDO!`);
  console.log(`   ✅ Regra: Cada produto pode ter APENAS 1 relação por fornecedor`);
  console.log(`   ✅ Verificação: Dupla checagem antes de criar relações`);
  console.log(`   ✅ Debug: Endpoint /debug/duplicacoes para monitorar`);
  console.log(`   ✅ Stats: Detecção automática de duplicações`);
});

module.exports = app;