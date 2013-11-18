/*jslint node: true, white: true */
module.exports = function (app) {
	'use strict';

	var async = require('async'),

		/**
		 *  Base de données
		 *  @todo support multi base et dynamique
		 *
		 *  @type {Object}
		 */
		db = app.database.use(app.config.get('migrations:db')),

		/**
		 *	@property logger Instance du logger spécifique à l'objet migrations
		 */
		logger = app.logger.child({component: 'Migrations'}),

		/**
		 *  Objet contenant les migrations.
		 *
		 *  @type {Object}
		 */
		migrations = {
			/**
			 *  Nom du champ de version dans les documents
			 *
			 *  @type {String}
			 */
			version_field: 'migration_version',

			/**
			 *  Numéro de la dernière version trouvée dans les fichiers (auto)
			 *
			 *  @type {Number}
			 */
			last: null,

			/**
			 *  Est-ce que les fichiers de migration sont chargés?
			 *
			 *  @type {Boolean}
			 */
			loaded: false,

			/**
			 *  Liste des objets d'étapes de migrations, chargé à partir des fichiers de migration
			 *
			 *  @type {Array}
			 */
			steps: []
		},

		/**
		 *  Données de progression
		 *
		 *  @type {Object}
		 */
		progress = {
			done: 0,
			total: 0,
			written: 0,
			intervalId: null
		},

		/**
		 *  Execution des migrations sur un document.
		 *
		 *  @param  {Object}   doc      Document à migrer
		 *  @param  {Number}   up_to	Version à atteindre
		 *  @param  {Function} callback
		 */
		migrateDocument = function(doc, up_to, callback, modified) {
			var version = doc[migrations.version_field] || 0,
				migration = migrations.steps[version];

			if (version < up_to && typeof migration.up === 'function') {
				migration.up(doc, function(new_doc) {
					doc = new_doc || doc;

					doc[migrations.version_field] = migration.id;

					migrateDocument(
						doc,
						up_to,
						callback,
						modified || new_doc !== null
					);
				});
			} else {
				callback(doc, modified);
			}
		},

		/**
		 *  Traitement des documents à mettre à jour par batch.
		 *
		 *  @param  {Number}   up_to      Version à atteindre
		 *  @param  {Number}   limit      Nom de documents à traiter par passe
		 *  @param  {Function} callback
		 */
		treatDocuments = function(up_to, limit, callback) {
			var filter = {
				include_docs: true,
				limit: limit,
				endkey: up_to - 1
			};

			db.view('migrations', 'list', filter, function (err, result) {
				if (err) {
					return callback(new Error('Unable to retrive data: ' + err));
				}

				if (!result.rows.length) {
					return callback();
				}

				async.eachLimit(
					result.rows,
					32,
					function(row, next) {
						migrateDocument(row.doc, up_to, function(doc, modified) {
							if (typeof doc === 'object') {
								db.insert(doc, function() {
									progress.done += 1;
									progress.written += modified ? 1 : 0;
									return next();
								});
							} else {
								progress.done += 1;
								return next();
							}
						});
					},
					function(err) {
						treatDocuments(up_to, limit, callback);
					}
				);
			});
		},

		/**
		 *  Version privée de doMigrations, une fois les migrations chargées.
		 *
		 *  @param  {Number}   up_to
		 *  @param  {Function} callback
		 */
		executeMigrations = function(up_to, callback) {
			if (migrations.last === null) {
				return callback(new Error('No migrations found'));
			}

			up_to = up_to || migrations.last;

			logger.info("Execution des migrations jusqu'à la v" + up_to);

			db.view('migrations', 'list', {endkey: up_to -1}, function (err, result) {
				if (err) {
					return callback(new Error('Unable to retrive data: ' + err));
				}

				progress.total = result.rows.length;
				result = null;

				progress.intervalId = setInterval(
					function() {
						logger.info('Migration: ' + progress.done + '/' + progress.total);
					},
					5000
				);

				treatDocuments(up_to, 512, function() {
					logger.info('Migrations terminées, ' + progress.done + ' documents traités, ' + progress.written + ' documents modifiés.');
					clearInterval(progress.intervalId);
					return callback();
				});
			});
		},

		/**
		 *  Execution des migrations sur les documents de la base
		 *
		 *  @param  {Number}    up_to     Numéro de la migration maximale, toutes si null.
		 *  @param  {Function}  callback
		 *
		 *  @return {Object}       Error si erreur, null sinon.
		 */
		doMigrations = function(up_to, callback) {
			up_to = up_to || null;
			callback = callback || function() {};

			if (!migrations.loaded) {
				loadFiles(function() {
					return executeMigrations(up_to, callback);
				});
			} else {
				executeMigrations(up_to, callback);
			}

			return null;
		},

		/**
		 *	Charge les fichiers de migration.
		 *
		 *	@param callback Callback de fin de chargement
		 */
		loadFiles = function (callback) {
			var loader_migrations = require('g-autoloader')({
				logger:		logger,
				composants_path:	app.path + app.config.get('directories:migrations'),
				imports: {
					migrations:		migrations
				}
			});

			loader_migrations.loadFiles(function() {
				migrations.steps.sort();
				migrations.last = migrations.steps[migrations.steps.length-1].id || null;

				return callback();
			});
		};

	/**
	 *	Exposition des attributs publiques.
	 */
	return {
		loadFiles:		loadFiles,
		doMigrations:	doMigrations
	};
};
